"""A worker a rendszer szíve: consumer groupban fogyasztja a parcel.events
topicot, futtatja az állapotgépet, és a Redis materializált nézetet írja.

Kézbesítési garanciák (tananyag!):
  * at-least-once: kézi commit CSAK sikeres feldolgozás után → üzenet nem
    veszik el, de újrakézbesítés előfordulhat;
  * idempotencia: a RedisStore event_id alapú dedupja a duplikátumokat
    kiszűri → a kettő együtt "gyakorlatilag exactly-once";
  * poison pill: a parse-olhatatlan vagy érvénytelen átmenetű üzenet DLQ-ba
    megy, és NEM blokkolja a folyamot.

Több replika esetén a Kafka a partíciókat szétosztja a workerek között
(rebalance) — a RebalanceLogger ezt logolja, `kubectl logs`-szal figyelhető.
"""
import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.errors import CommitFailedError
from prometheus_client import start_http_server
from pydantic import ValidationError

from ..config import Settings, get_settings
from ..kafka_client import create_producer
from ..metrics import (
    DLQ_EVENTS,
    EVENTS_CONSUMED,
    PARCELS_BY_STATUS,
    PARCELS_TOTAL,
    PROCESSING_TIME,
    WORKER_READY,
)
from ..models import InvalidTransitionError, ParcelEvent, ParcelStatus
from ..redis_store import RedisStore

log = logging.getLogger(__name__)


class RebalanceLogger(ConsumerRebalanceListener):
    """Partíció-átrendezések logolása — skálázási demókhoz aranyat ér."""

    async def on_partitions_revoked(self, revoked) -> None:
        if revoked:
            log.info(
                "Rebalance: elvett partíciók: %s",
                sorted(tp.partition for tp in revoked),
            )

    async def on_partitions_assigned(self, assigned) -> None:
        log.info(
            "Rebalance: hozzám rendelt partíciók: %s",
            sorted(tp.partition for tp in assigned),
        )


async def send_to_dlq(
    producer: AIOKafkaProducer,
    settings: Settings,
    raw_value: bytes,
    reason: str,
    detail: str,
) -> None:
    """A hibás üzenetet borítékba csomagolva tesszük a DLQ-ra, hogy a hiba
    oka és az eredeti üzenet is megmaradjon a későbbi elemzéshez."""
    envelope = json.dumps(
        {
            "reason": reason,
            "detail": detail[:500],
            "original": raw_value.decode("utf-8", errors="replace"),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=False,
    ).encode("utf-8")
    await producer.send_and_wait(settings.kafka_dlq_topic, envelope)
    DLQ_EVENTS.labels(reason).inc()


async def handle_message(
    msg,
    store: RedisStore,
    producer: AIOKafkaProducer,
    settings: Settings,
) -> None:
    started = time.perf_counter()
    # Null értékű (tombstone) üzenetre a DLQ-ág is elszállna (None.decode) —
    # üres bytes-ként kezeljük, így a normál validációs úton bukik el.
    raw_value = msg.value if msg.value is not None else b""
    try:
        event = ParcelEvent.model_validate_json(raw_value)
    except ValidationError as exc:
        log.warning("Érvénytelen üzenet (partíció=%s offset=%s) → DLQ", msg.partition, msg.offset)
        await send_to_dlq(producer, settings, raw_value, "validacios_hiba", str(exc))
        await store.incr_dlq("validacios_hiba")
        EVENTS_CONSUMED.labels("ismeretlen", "dlq").inc()
        return

    try:
        result = await store.apply_event(event)
    except InvalidTransitionError as exc:
        log.warning("%s (csomag=%s) → DLQ", exc, event.parcel_id)
        await send_to_dlq(producer, settings, raw_value, "ervenytelen_allapotatmenet", str(exc))
        await store.incr_dlq("ervenytelen_allapotatmenet")
        EVENTS_CONSUMED.labels(event.event_type.value, "dlq").inc()
        return

    EVENTS_CONSUMED.labels(event.event_type.value, result).inc()
    PROCESSING_TIME.observe(time.perf_counter() - started)
    if result == "duplicate":
        log.info("Duplikált esemény kiszűrve: %s", event.event_id)


async def stats_refresher(store: RedisStore, stop_event: asyncio.Event) -> None:
    """Redis állapot → Prometheus gauge-ok, 15 másodpercenként."""
    while not stop_event.is_set():
        try:
            counts = await store.status_counts()
            for status in ParcelStatus:
                PARCELS_BY_STATUS.labels(status.value).set(counts.get(status.value, 0))
            PARCELS_TOTAL.set(await store.parcels_total())
        except Exception:  # noqa: BLE001 - a metrika-frissítés ne döntse le a workert
            log.warning("Statisztika-frissítés sikertelen", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=15)
        except asyncio.TimeoutError:
            pass


async def start_consumer_with_retry(
    consumer: AIOKafkaConsumer, retries: int = 90, delay_seconds: float = 2.0
) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            await consumer.start()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log.warning("Kafka még nem érhető el (%d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(delay_seconds)
    raise RuntimeError("Kafka consumer nem tudott elindulni") from last_error


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Prometheus /metrics — a liveness probe is ezt éri el.
    start_http_server(settings.metrics_port)

    store = RedisStore.from_url(
        settings.redis_url,
        dedup_ttl_seconds=settings.dedup_ttl_seconds,
        cache_ttl_seconds=settings.stats_cache_ttl_seconds,
        parcel_ttl_seconds=settings.parcel_ttl_seconds,
    )
    producer = await create_producer()

    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,  # kézi commit → at-least-once
        # Új (vagy resetelt) consumer group a topic elejéről indul. Teljes
        # visszajátszás előtt a Redist is üríteni kell (FLUSHALL), különben
        # a dedup kulcsok kiszűrnék az újrajátszott eseményeket.
        auto_offset_reset="earliest",
    )
    await start_consumer_with_retry(consumer)
    consumer.subscribe([settings.kafka_topic], listener=RebalanceLogger())

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # SIGTERM: a Kubernetes így kér szép leállást (graceful shutdown).
        loop.add_signal_handler(sig, stop_event.set)

    refresher = asyncio.create_task(stats_refresher(store, stop_event))
    WORKER_READY.set(1)
    log.info(
        "Worker elindult, topic=%s group=%s",
        settings.kafka_topic,
        settings.kafka_consumer_group,
    )

    try:
        while not stop_event.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=100)
            for _tp, messages in batch.items():
                for msg in messages:
                    await handle_message(msg, store, producer, settings)
            if batch:
                # Kötegelt commit a feldolgozás UTÁN — ha itt halnánk meg,
                # a köteg újrajátszódik, de a dedup kiszűri (idempotencia).
                try:
                    await consumer.commit()
                except CommitFailedError:
                    # Rebalance történt a getmany és a commit között (pl. épp
                    # skálázunk): a köteg újrajátszódik az új gazdánál, a
                    # dedup kiszűri — nem ok a worker leállására.
                    log.warning("Offset commit sikertelen (rebalance) — a köteg újrajátszódik")
    finally:
        WORKER_READY.set(0)
        stop_event.set()
        await refresher
        await consumer.stop()
        await producer.stop()
        await store.close()
        log.info("Worker leállt (rendezetten)")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
