"""A Redis-réteg tesztjei fakeredis-szel (nem kell futó Redis)."""
import pytest

from parcel_tracker.models import (
    EventType,
    InvalidTransitionError,
    ParcelEvent,
    new_parcel_id,
)


def make_event(parcel_id, event_type, location="Budapest OLK", **payload):
    return ParcelEvent(
        parcel_id=parcel_id, event_type=event_type, location=location, payload=payload
    )


async def test_apply_event_creates_parcel(store):
    parcel_id = new_parcel_id()
    result = await store.apply_event(
        make_event(parcel_id, EventType.FELVETEL, destination="Szeged", weight_g=900)
    )
    assert result == "applied"
    parcel = await store.get_parcel(parcel_id)
    assert parcel["status"] == "FELVEVE"
    assert parcel["destination"] == "Szeged"
    history = await store.get_history(parcel_id)
    assert len(history) == 1
    assert history[0]["event_type"] == "FELVETEL"
    # A csomagadatnak lejárata van (a Redis nem telhet be).
    assert await store._redis.ttl(f"parcel:{parcel_id}") > 0


async def test_duplicate_event_is_skipped(store):
    """Ugyanaz az event_id kétszer → a második "duplicate" (idempotencia)."""
    parcel_id = new_parcel_id()
    event = make_event(parcel_id, EventType.FELVETEL, destination="Pécs", weight_g=100)
    assert await store.apply_event(event) == "applied"
    assert await store.apply_event(event) == "duplicate"
    parcel = await store.get_parcel(parcel_id)
    assert parcel["events_count"] == "1"


async def test_invalid_transition_raises(store):
    parcel_id = new_parcel_id()
    await store.apply_event(make_event(parcel_id, EventType.FELVETEL, destination="Győr"))
    with pytest.raises(InvalidTransitionError):
        await store.apply_event(make_event(parcel_id, EventType.KEZBESITVE))


async def test_status_counts_move_between_statuses(store):
    parcel_id = new_parcel_id()
    await store.apply_event(make_event(parcel_id, EventType.FELVETEL, destination="Buda"))
    await store.apply_event(make_event(parcel_id, EventType.FELDOLGOZAS))
    counts = await store.status_counts()
    assert counts.get("FELVEVE", 0) == 0
    assert counts["FELDOLGOZAS_ALATT"] == 1


async def test_overview_and_cache(store):
    parcel_id = new_parcel_id()
    await store.apply_event(make_event(parcel_id, EventType.FELVETEL, destination="Eger"))
    first = await store.get_overview()
    assert first["cache"] == "miss"
    assert first["parcels_total"] == 1
    second = await store.get_overview()
    assert second["cache"] == "hit"
    assert second["parcels_total"] == 1


async def test_full_lifecycle_updates_delivered_counter(store):
    parcel_id = new_parcel_id()
    for event_type in [
        EventType.FELVETEL,
        EventType.FELDOLGOZAS,
        EventType.SZALLITAS,
        EventType.KEZBESITES_INDITVA,
        EventType.KEZBESITVE,
    ]:
        await store.apply_event(make_event(parcel_id, event_type, destination="Vác"))
    parcel = await store.get_parcel(parcel_id)
    assert parcel["status"] == "KEZBESITVE"
    overview = await store.get_overview()
    assert overview["delivered_today"] == 1
    assert overview["active_parcels"] == 0


async def test_dlq_counter(store):
    await store.incr_dlq("validacios_hiba")
    await store.incr_dlq("validacios_hiba")
    overview = await store.get_overview()
    assert overview["dlq_total"] == 2
