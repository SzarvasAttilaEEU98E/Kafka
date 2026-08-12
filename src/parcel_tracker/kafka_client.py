"""Kafka producer segédfüggvények (aiokafka).

A producer idempotens (enable_idempotence=True) és minden replika
visszaigazolására vár (acks="all") — ez a "ne veszítsünk üzenetet"
alapbeállítás. Az üzenet kulcsa a parcel_id: azonos csomag eseményei így
mindig ugyanarra a partícióra kerülnek, ami partíción belül sorrendet
garantál.
"""
import asyncio
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer

from .config import get_settings
from .models import ParcelEvent

log = logging.getLogger(__name__)


async def create_producer(
    bootstrap: Optional[str] = None,
    retries: int = 90,
    delay_seconds: float = 2.0,
) -> AIOKafkaProducer:
    """Producer indítása újrapróbálkozással.

    Kubernetesben az alkalmazás gyakran hamarabb indul, mint a Kafka broker —
    ahelyett, hogy elszállnánk (CrashLoopBackOff), türelmesen várunk rá.
    """
    settings = get_settings()
    servers = bootstrap or settings.kafka_bootstrap_servers
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        producer = AIOKafkaProducer(
            bootstrap_servers=servers,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
        )
        try:
            await producer.start()
            log.info("Kafka producer elindult (%s)", servers)
            return producer
        except Exception as exc:  # noqa: BLE001 - induláskor bármi jöhet
            last_error = exc
            await producer.stop()
            log.warning(
                "Kafka még nem érhető el (%d/%d próbálkozás): %s", attempt, retries, exc
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError(f"Kafka nem érhető el: {servers}") from last_error


async def send_event(producer: AIOKafkaProducer, topic: str, event: ParcelEvent) -> None:
    """Esemény beküldése; a kulcs a parcel_id (partíción belüli sorrendhez)."""
    await producer.send_and_wait(
        topic,
        value=event.to_kafka_bytes(),
        key=event.parcel_id.encode("utf-8"),
    )
