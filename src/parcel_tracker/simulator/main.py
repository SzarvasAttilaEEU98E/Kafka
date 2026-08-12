"""Forgalom-szimulátor.

Szándékosan a publikus HTTP API-n keresztül dolgozik (nem közvetlenül
Kafkába ír): így a teljes út terhelődik — HTTP → API → Kafka → worker →
Redis —, és a demó "magától él".

A SIM_CHAOS_PERCENT arányban szándékosan hibás eseménysorrendet küld
(pl. KEZBESITVE közvetlenül FELVETEL után), hogy a DLQ működése látható
legyen a dashboardon és a Grafanán.
"""
import asyncio
import logging
import random
from typing import Any, Dict, Optional, Set

import httpx

from ..config import Settings, get_settings
from ..models import DESTINATIONS, SORTING_CENTERS, EventType

log = logging.getLogger(__name__)

SENDERS = [
    "Kovács Anna",
    "Nagy Béla",
    "Szabó Csilla",
    "Tóth Dániel",
    "Horváth Emma",
    "Kiss Ferenc",
    "Molnár Gábor",
    "Varga Hanna",
]

FAIL_REASONS = [
    "A címzett nem elérhető",
    "Hibás cím",
    "Átvétel megtagadva",
]


async def wait_for_api(client: httpx.AsyncClient) -> None:
    while True:
        try:
            response = await client.get("/readyz")
            if response.status_code == 200:
                log.info("API elérhető, indul a forgalom")
                return
        except httpx.HTTPError:
            pass
        log.info("Várakozás az API-ra...")
        await asyncio.sleep(3)


async def post_event(
    client: httpx.AsyncClient,
    parcel_id: str,
    event_type: EventType,
    location: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    response = await client.post(
        f"/api/parcels/{parcel_id}/events",
        json={"event_type": event_type.value, "location": location, "payload": payload or {}},
    )
    response.raise_for_status()


async def step_delay() -> None:
    """Életszerű, de demóhoz elég gyors késleltetés a lépések közt."""
    await asyncio.sleep(random.uniform(2.0, 8.0))


async def run_parcel(client: httpx.AsyncClient, settings: Settings) -> None:
    """Egy csomag teljes életútja feladástól kézbesítésig."""
    try:
        destination = random.choice(DESTINATIONS)
        response = await client.post(
            "/api/parcels",
            json={
                "destination": destination,
                "weight_g": random.randint(100, 20000),
                "sender": random.choice(SENDERS),
            },
        )
        response.raise_for_status()
        parcel_id = response.json()["parcel_id"]

        if random.uniform(0, 100) < settings.sim_chaos_percent:
            # Káosz: sorrendtörő esemény → a worker DLQ-ba teszi. A csomag
            # állapota NEM változik (a hibás esemény nem íródik rá), ezért
            # utána a normál életút gond nélkül folytatódik — pont, mint
            # amikor élesben egy hibás integráció küld szemetet.
            await step_delay()
            await post_event(client, parcel_id, EventType.KEZBESITVE, "Káosz Kft.")
            log.info("CHAOS: sorrendtörő esemény beküldve (%s)", parcel_id)

        for center in random.sample(SORTING_CENTERS, k=random.choice([1, 1, 2])):
            await step_delay()
            await post_event(client, parcel_id, EventType.FELDOLGOZAS, center)

        await step_delay()
        await post_event(client, parcel_id, EventType.SZALLITAS, f"Úton {destination} felé")
        await step_delay()
        await post_event(
            client, parcel_id, EventType.KEZBESITES_INDITVA, f"{destination} kézbesítő pont"
        )
        await step_delay()

        if random.random() < 0.9:
            await post_event(client, parcel_id, EventType.KEZBESITVE, destination)
            return

        # Sikertelen kézbesítés → újrapróbálkozás vagy visszaküldés.
        await post_event(
            client,
            parcel_id,
            EventType.SIKERTELEN_KEZBESITES,
            destination,
            payload={"reason": random.choice(FAIL_REASONS)},
        )
        await step_delay()
        if random.random() < 0.7:
            await post_event(
                client, parcel_id, EventType.KEZBESITES_INDITVA, f"{destination} kézbesítő pont"
            )
            await step_delay()
            await post_event(client, parcel_id, EventType.KEZBESITVE, destination)
        else:
            await post_event(client, parcel_id, EventType.VISSZAKULDES, "Vissza a feladóhoz")
    except httpx.HTTPError as exc:
        log.warning("Csomag-életciklus megszakadt: %s", exc)


async def overview_logger(client: httpx.AsyncClient, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            response = await client.get("/api/stats/overview")
            if response.status_code == 200:
                data = response.json()
                log.info(
                    "Áttekintés: összes=%s aktív=%s ma kézbesítve=%s dlq=%s",
                    data.get("parcels_total"),
                    data.get("active_parcels"),
                    data.get("delivered_today"),
                    data.get("dlq_total"),
                )
        except httpx.HTTPError:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    interval = 60.0 / max(settings.sim_parcels_per_minute, 0.1)
    log.info(
        "Szimulátor indul: %.1f csomag/perc, káosz=%.1f%%, cél=%s",
        settings.sim_parcels_per_minute,
        settings.sim_chaos_percent,
        settings.api_base_url,
    )

    stop_event = asyncio.Event()
    tasks: Set[asyncio.Task] = set()

    async with httpx.AsyncClient(base_url=settings.api_base_url, timeout=10.0) as client:
        await wait_for_api(client)
        logger_task = asyncio.create_task(overview_logger(client, stop_event))
        try:
            while True:
                task = asyncio.create_task(run_parcel(client, settings))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
                await asyncio.sleep(interval * random.uniform(0.5, 1.5))
        finally:
            stop_event.set()
            logger_task.cancel()
            for task in tasks:
                task.cancel()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
