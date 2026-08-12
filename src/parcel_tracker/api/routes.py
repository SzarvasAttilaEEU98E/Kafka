"""REST végpontok.

Fontos tervezési döntés: az író végpontok NEM írnak Redist és nem
ellenőriznek állapotátmenetet — csak Kafkába termelnek, és 202 Accepted-del
válaszolnak. A feldolgozás aszinkron, a worker végzi. Ezért előfordulhat,
hogy egy frissen feladott csomag lekérdezése pár tíz ms-ig még 404-et ad:
ez az eventual consistency, nem hiba.
"""
from typing import Any, Dict

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..config import get_settings
from ..kafka_client import send_event
from ..metrics import EVENTS_PRODUCED
from ..models import PARCEL_ID_PATTERN, EventType, ParcelEvent, new_parcel_id
from ..redis_store import RedisStore

router = APIRouter()


class CreateParcelRequest(BaseModel):
    destination: str = Field(min_length=2, max_length=64, description="Célváros")
    weight_g: int = Field(gt=0, le=31500, description="Súly grammban (max 31,5 kg)")
    sender: str = Field(default="", max_length=64)


class AppendEventRequest(BaseModel):
    event_type: EventType
    location: str = Field(default="", max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict)


def _store(request: Request) -> RedisStore:
    return request.app.state.store


def _producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.producer


@router.post("/parcels", status_code=202)
async def create_parcel(body: CreateParcelRequest, request: Request) -> Dict[str, str]:
    """Csomag feladása → FELVETEL esemény a Kafka topicra."""
    settings = get_settings()
    event = ParcelEvent(
        parcel_id=new_parcel_id(),
        event_type=EventType.FELVETEL,
        location="Felvételi pont",
        payload={
            "destination": body.destination,
            "weight_g": body.weight_g,
            "sender": body.sender,
        },
    )
    await send_event(_producer(request), settings.kafka_topic, event)
    EVENTS_PRODUCED.labels(event.event_type.value).inc()
    return {
        "parcel_id": event.parcel_id,
        "event_id": event.event_id,
        "status": "elfogadva",
        "info": "Az esemény aszinkron dolgozódik fel, a lekérdezés pár ms múlva látja.",
    }


@router.post("/parcels/{parcel_id}/events", status_code=202)
async def append_event(
    parcel_id: str, body: AppendEventRequest, request: Request
) -> Dict[str, str]:
    """Új életút-esemény beküldése egy csomaghoz.

    Szándékosan nem ellenőrizzük itt az állapotgépet: ha az átmenet
    érvénytelen, a worker DLQ-ba teszi — így demózható a poison pill kezelés.
    """
    if not PARCEL_ID_PATTERN.match(parcel_id):
        raise HTTPException(status_code=400, detail="Érvénytelen csomagazonosító formátum")
    settings = get_settings()
    event = ParcelEvent(
        parcel_id=parcel_id,
        event_type=body.event_type,
        location=body.location,
        payload=body.payload,
    )
    await send_event(_producer(request), settings.kafka_topic, event)
    EVENTS_PRODUCED.labels(event.event_type.value).inc()
    return {"event_id": event.event_id, "status": "elfogadva"}


@router.get("/parcels/{parcel_id}")
async def get_parcel(parcel_id: str, request: Request) -> Dict[str, Any]:
    if not PARCEL_ID_PATTERN.match(parcel_id):
        raise HTTPException(status_code=400, detail="Érvénytelen csomagazonosító formátum")
    store = _store(request)
    parcel = await store.get_parcel(parcel_id)
    if parcel is None:
        raise HTTPException(
            status_code=404,
            detail="Ismeretlen csomag (vagy az esemény még feldolgozás alatt van)",
        )
    history = await store.get_history(parcel_id, limit=20)
    return {"parcel": parcel, "history": history}


@router.get("/parcels/{parcel_id}/history")
async def get_history(parcel_id: str, request: Request) -> Dict[str, Any]:
    if not PARCEL_ID_PATTERN.match(parcel_id):
        raise HTTPException(status_code=400, detail="Érvénytelen csomagazonosító formátum")
    history = await _store(request).get_history(parcel_id, limit=200)
    if not history:
        raise HTTPException(status_code=404, detail="Nincs esemény ehhez a csomaghoz")
    return {"parcel_id": parcel_id, "history": history}


@router.get("/stats/overview")
async def stats_overview(request: Request) -> Dict[str, Any]:
    """Összesített statisztika (cache-aside: figyeld a "cache" mezőt!)."""
    return await _store(request).get_overview()


@router.get("/live")
async def live_feed(request: Request) -> StreamingResponse:
    """Server-Sent Events folyam a Redis Pub/Sub-ból — élő dashboardhoz."""
    store = _store(request)

    async def event_stream():
        yield "retry: 3000\n\n"
        async for item in store.live_events():
            if item is None:
                yield ": ping\n\n"  # heartbeat, hogy a proxy ne zárja le
            else:
                yield f"data: {item}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
