"""Redis-réteg: materializált nézet, statisztikák, cache és élő közvetítés.

Redis adatszerkezetek a projektben (mindegyik más-más tanulási célt szolgál):

===================================  =========  ==========================================
Kulcs                                Típus      Szerep
===================================  =========  ==========================================
parcel:{id}                          HASH       csomag aktuális állapota
parcel:{id}:history                  LIST       esemény-idővonal (RPUSH, kronologikus)
dedup:{event_id}                     STRING     idempotencia-kulcs (SET NX + TTL)
stats:parcels:total                  STRING     számláló
stats:status_counts                  HASH       csomagok száma státuszonként
stats:center:events                  ZSET       feldolgozó központok "toplistája"
stats:delivered:{nap}                STRING     napi kézbesítés-számláló (TTL 40 nap)
stats:dlq:total / stats:dlq:reasons  STR/HASH   DLQ számlálók
cache:stats:overview                 STRING     cache-aside minta (rövid TTL)
events:live                          PUB/SUB    élő esemény-közvetítés a dashboardnak
===================================  =========  ==========================================
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import redis.asyncio as aioredis

from .models import EventType, ParcelEvent, ParcelStatus, validate_transition

log = logging.getLogger(__name__)

LIVE_CHANNEL = "events:live"

#: Nem végállapotú státuszok — az "aktív csomag" számításhoz.
ACTIVE_STATUSES = {
    ParcelStatus.FELVEVE,
    ParcelStatus.FELDOLGOZAS_ALATT,
    ParcelStatus.SZALLITAS_ALATT,
    ParcelStatus.KEZBESITES_ALATT,
    ParcelStatus.SIKERTELEN_KEZBESITES,
}


class RedisStore:
    def __init__(
        self,
        client: "aioredis.Redis",
        dedup_ttl_seconds: int = 86400,
        cache_ttl_seconds: int = 5,
        parcel_ttl_seconds: int = 259200,
    ):
        self._redis = client
        self._dedup_ttl = dedup_ttl_seconds
        self._cache_ttl = cache_ttl_seconds
        self._parcel_ttl = parcel_ttl_seconds

    @classmethod
    def from_url(
        cls,
        url: str,
        dedup_ttl_seconds: int = 86400,
        cache_ttl_seconds: int = 5,
        parcel_ttl_seconds: int = 259200,
    ) -> "RedisStore":
        client = aioredis.from_url(url, decode_responses=True)
        return cls(client, dedup_ttl_seconds, cache_ttl_seconds, parcel_ttl_seconds)

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def close(self) -> None:
        await self._redis.aclose()

    # ------------------------------------------------------------------ írás

    async def apply_event(self, event: ParcelEvent) -> str:
        """Esemény lejátszása a materializált nézetre.

        Visszatérés: "applied" vagy "duplicate". Érvénytelen állapotátmenetre
        InvalidTransitionError-t dob (a hívó dönt a DLQ-ról).

        Sorrend: előbb validálunk, és csak utána foglaljuk le a dedup kulcsot
        (SET NX) — így a DLQ-ba került esemény event_id-je szabad marad, és
        egy későbbi DLQ-visszajátszás (redrive) újra feldolgozható. Ha a
        worker a foglalás és az állapotírás között halna meg, az esemény
        elveszhet — ez tudatos egyszerűsítés, az oktatási anyag tárgyalja,
        hogyan lehetne szigorítani (Lua szkript, Kafka tranzakciók).
        """
        parcel_key = f"parcel:{event.parcel_id}"
        current_raw = await self._redis.hget(parcel_key, "status")
        current = ParcelStatus(current_raw) if current_raw else None
        new_status = validate_transition(current, event.event_type)  # dobhat

        reserved = await self._redis.set(
            f"dedup:{event.event_id}", "1", nx=True, ex=self._dedup_ttl
        )
        if not reserved:
            return "duplicate"

        occurred_iso = event.occurred_at.isoformat()
        history_entry = json.dumps(
            {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "status": new_status.value,
                "location": event.location,
                "occurred_at": occurred_iso,
                "payload": event.payload,
            },
            ensure_ascii=False,
        )

        pipe = self._redis.pipeline(transaction=True)
        pipe.hset(
            parcel_key,
            mapping={
                "parcel_id": event.parcel_id,
                "status": new_status.value,
                "current_location": event.location,
                "updated_at": occurred_iso,
            },
        )
        if event.event_type == EventType.FELVETEL:
            pipe.hset(
                parcel_key,
                mapping={
                    "created_at": occurred_iso,
                    "destination": str(event.payload.get("destination", "")),
                    "weight_g": str(event.payload.get("weight_g", "")),
                    "sender": str(event.payload.get("sender", "")),
                },
            )
            pipe.incr("stats:parcels:total")
        pipe.hincrby(parcel_key, "events_count", 1)
        pipe.rpush(f"parcel:{event.parcel_id}:history", history_entry)
        # Demó adat: TTL-lel gondoskodunk róla, hogy a Redis ne teljen be
        # (maxmemory + noeviction mellett a betelt memória írási hiba lenne).
        pipe.expire(parcel_key, self._parcel_ttl)
        pipe.expire(f"parcel:{event.parcel_id}:history", self._parcel_ttl)

        # Státusz-megoszlás karbantartása: a régiből ki, az újba be.
        if current is not None:
            pipe.hincrby("stats:status_counts", current.value, -1)
        pipe.hincrby("stats:status_counts", new_status.value, 1)

        # A toplista a feldolgozó központok forgalmát méri, ezért csak a
        # FELDOLGOZAS eseményeket számoljuk (a felvételi/kézbesítő pontok
        # elnyomnák a valódi központokat).
        if event.location and event.event_type == EventType.FELDOLGOZAS:
            pipe.zincrby("stats:center:events", 1, event.location)

        if new_status == ParcelStatus.KEZBESITVE:
            day_key = f"stats:delivered:{event.occurred_at.date().isoformat()}"
            pipe.incr(day_key)
            pipe.expire(day_key, 40 * 86400)

        pipe.publish(
            LIVE_CHANNEL,
            json.dumps(
                {
                    "parcel_id": event.parcel_id,
                    "event_type": event.event_type.value,
                    "status": new_status.value,
                    "location": event.location,
                    "occurred_at": occurred_iso,
                },
                ensure_ascii=False,
            ),
        )
        await pipe.execute()
        return "applied"

    async def incr_dlq(self, reason: str) -> None:
        pipe = self._redis.pipeline(transaction=True)
        pipe.incr("stats:dlq:total")
        pipe.hincrby("stats:dlq:reasons", reason, 1)
        await pipe.execute()

    # --------------------------------------------------------------- olvasás

    async def get_parcel(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        data = await self._redis.hgetall(f"parcel:{parcel_id}")
        return data or None

    async def get_history(self, parcel_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        raw = await self._redis.lrange(f"parcel:{parcel_id}:history", -limit, -1)
        return [json.loads(item) for item in raw]

    async def get_overview(self) -> Dict[str, Any]:
        """Összesítő statisztika cache-aside mintával.

        Első hívás: kiszámoljuk Redisből és rövid TTL-lel cache-eljük.
        További hívások a TTL lejártáig a cache-ből jönnek — a dashboard
        többezer kliens mellett sem terhelné a "drága" aggregációt.
        """
        cached = await self._redis.get("cache:stats:overview")
        if cached:
            result = json.loads(cached)
            result["cache"] = "hit"
            return result

        status_counts_raw = await self._redis.hgetall("stats:status_counts")
        status_counts = {
            status: int(count)
            for status, count in status_counts_raw.items()
            if int(count) > 0
        }
        top_raw = await self._redis.zrevrange("stats:center:events", 0, 9, withscores=True)
        top_centers = [
            {"name": name, "events": int(score)} for name, score in top_raw
        ]
        today = datetime.now(timezone.utc).date().isoformat()
        total = int(await self._redis.get("stats:parcels:total") or 0)
        delivered_today = int(await self._redis.get(f"stats:delivered:{today}") or 0)
        dlq_total = int(await self._redis.get("stats:dlq:total") or 0)
        active = sum(
            count
            for status, count in status_counts.items()
            if ParcelStatus(status) in ACTIVE_STATUSES
        )

        result = {
            "parcels_total": total,
            "active_parcels": active,
            "delivered_today": delivered_today,
            "dlq_total": dlq_total,
            "status_counts": status_counts,
            "top_centers": top_centers,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache": "miss",
        }
        await self._redis.set(
            "cache:stats:overview", json.dumps(result, ensure_ascii=False), ex=self._cache_ttl
        )
        return result

    async def status_counts(self) -> Dict[str, int]:
        raw = await self._redis.hgetall("stats:status_counts")
        return {status: int(count) for status, count in raw.items()}

    async def parcels_total(self) -> int:
        return int(await self._redis.get("stats:parcels:total") or 0)

    # ------------------------------------------------------------ élő adás

    async def live_events(self) -> AsyncIterator[Optional[str]]:
        """Pub/sub feliratkozás; timeoutnál None-t ad (heartbeat lehetőség)."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(LIVE_CHANNEL)
        try:
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=15.0
                )
                if message is None:
                    yield None
                elif message.get("type") == "message":
                    yield message["data"]
        finally:
            try:
                await pubsub.unsubscribe(LIVE_CHANNEL)
                await pubsub.aclose()
            except Exception:  # noqa: BLE001 - lezáráskor már nem érdekes
                log.debug("PubSub lezárása közben hiba", exc_info=True)
