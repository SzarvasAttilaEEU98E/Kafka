"""API tesztek: valódi Kafka és Redis nélkül (FakeProducer + fakeredis).

A lifespan-t nem futtatjuk — az app.state-et kézzel töltjük fel, így a
végpontok izoláltan tesztelhetők.
"""
import httpx
import pytest

from parcel_tracker.api.main import create_app
from parcel_tracker.models import EventType, ParcelEvent, new_parcel_id

from .conftest import sent_events


@pytest.fixture
async def client(store, fake_producer):
    app = create_app()
    app.state.store = store
    app.state.producer = fake_producer
    app.state.ready = True
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as test_client:
        yield test_client


async def test_create_parcel_produces_kafka_event(client, fake_producer):
    response = await client.post(
        "/api/parcels", json={"destination": "Szeged", "weight_g": 1500}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["parcel_id"].startswith("PT")

    events = sent_events(fake_producer)
    assert len(events) == 1
    assert events[0].event_type == EventType.FELVETEL
    assert events[0].payload["destination"] == "Szeged"
    # A kulcs a parcel_id → partíción belüli sorrend garantált.
    assert fake_producer.sent[0]["key"] == body["parcel_id"].encode()


async def test_create_parcel_validates_weight(client):
    response = await client.post(
        "/api/parcels", json={"destination": "Szeged", "weight_g": -5}
    )
    assert response.status_code == 422


async def test_append_event_rejects_bad_parcel_id(client):
    response = await client.post(
        "/api/parcels/NEMJO/events",
        json={"event_type": "FELDOLGOZAS", "location": "Budapest OLK"},
    )
    assert response.status_code == 400


async def test_get_unknown_parcel_returns_404(client):
    response = await client.get(f"/api/parcels/{new_parcel_id()}")
    assert response.status_code == 404


async def test_get_parcel_after_worker_applied(client, store):
    """A "worker megcsinálta" utáni olvasási út."""
    parcel_id = new_parcel_id()
    await store.apply_event(
        ParcelEvent(
            parcel_id=parcel_id,
            event_type=EventType.FELVETEL,
            location="Felvételi pont",
            payload={"destination": "Debrecen", "weight_g": 800},
        )
    )
    response = await client.get(f"/api/parcels/{parcel_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["parcel"]["status"] == "FELVEVE"
    assert len(body["history"]) == 1


async def test_stats_overview(client):
    response = await client.get("/api/stats/overview")
    assert response.status_code == 200
    body = response.json()
    assert "parcels_total" in body
    assert body["cache"] in {"hit", "miss"}


async def test_readyz_and_healthz(client):
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).status_code == 200


async def test_metrics_endpoint(client):
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert b"parcel_events_produced_total" in response.content
