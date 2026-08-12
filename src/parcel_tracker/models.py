"""Domain modellek: csomagesemények és állapotgép.

A csomag életét eseményfolyam írja le (event sourcing gondolat): a forrás
igazsága a Kafka topic, a Redis csak ebből épített, bármikor újrajátszható
materializált nézet. Az állapotgép a workerben fut — az API szándékosan nem
ellenőrzi az átmeneteket, csak továbbítja az eseményt (CQRS: az írási és az
olvasási út szétválik).
"""
from __future__ import annotations

import random
import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Set

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    FELVETEL = "FELVETEL"
    FELDOLGOZAS = "FELDOLGOZAS"
    SZALLITAS = "SZALLITAS"
    KEZBESITES_INDITVA = "KEZBESITES_INDITVA"
    KEZBESITVE = "KEZBESITVE"
    SIKERTELEN_KEZBESITES = "SIKERTELEN_KEZBESITES"
    VISSZAKULDES = "VISSZAKULDES"


class ParcelStatus(str, Enum):
    FELVEVE = "FELVEVE"
    FELDOLGOZAS_ALATT = "FELDOLGOZAS_ALATT"
    SZALLITAS_ALATT = "SZALLITAS_ALATT"
    KEZBESITES_ALATT = "KEZBESITES_ALATT"
    KEZBESITVE = "KEZBESITVE"
    SIKERTELEN_KEZBESITES = "SIKERTELEN_KEZBESITES"
    VISSZAKULDVE = "VISSZAKULDVE"


#: Melyik esemény milyen státuszba viszi a csomagot.
EVENT_RESULT_STATUS: Dict[EventType, ParcelStatus] = {
    EventType.FELVETEL: ParcelStatus.FELVEVE,
    EventType.FELDOLGOZAS: ParcelStatus.FELDOLGOZAS_ALATT,
    EventType.SZALLITAS: ParcelStatus.SZALLITAS_ALATT,
    EventType.KEZBESITES_INDITVA: ParcelStatus.KEZBESITES_ALATT,
    EventType.KEZBESITVE: ParcelStatus.KEZBESITVE,
    EventType.SIKERTELEN_KEZBESITES: ParcelStatus.SIKERTELEN_KEZBESITES,
    EventType.VISSZAKULDES: ParcelStatus.VISSZAKULDVE,
}

#: Adott státuszból milyen események fogadhatók el. A ``None`` kulcs a még
#: nem létező csomagot jelenti. Egy csomag több feldolgozó központon is
#: áthaladhat, ezért a FELDOLGOZAS önmagából is megengedett.
ALLOWED_TRANSITIONS: Dict[Optional[ParcelStatus], Set[EventType]] = {
    None: {EventType.FELVETEL},
    ParcelStatus.FELVEVE: {EventType.FELDOLGOZAS},
    ParcelStatus.FELDOLGOZAS_ALATT: {EventType.FELDOLGOZAS, EventType.SZALLITAS},
    ParcelStatus.SZALLITAS_ALATT: {EventType.FELDOLGOZAS, EventType.KEZBESITES_INDITVA},
    ParcelStatus.KEZBESITES_ALATT: {EventType.KEZBESITVE, EventType.SIKERTELEN_KEZBESITES},
    ParcelStatus.SIKERTELEN_KEZBESITES: {EventType.KEZBESITES_INDITVA, EventType.VISSZAKULDES},
    ParcelStatus.KEZBESITVE: set(),
    ParcelStatus.VISSZAKULDVE: set(),
}


class InvalidTransitionError(Exception):
    """Az esemény az aktuális státuszból nem értelmezhető → DLQ."""

    def __init__(self, current: Optional[ParcelStatus], event_type: EventType):
        self.current = current
        self.event_type = event_type
        current_name = current.value if current else "NINCS_ILYEN_CSOMAG"
        super().__init__(
            f"Érvénytelen átmenet: {current_name} állapotból nem fogadható el "
            f"{event_type.value} esemény"
        )


def validate_transition(
    current: Optional[ParcelStatus], event_type: EventType
) -> ParcelStatus:
    """Visszaadja az új státuszt, vagy InvalidTransitionError-t dob."""
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if event_type not in allowed:
        raise InvalidTransitionError(current, event_type)
    return EVENT_RESULT_STATUS[event_type]


PARCEL_ID_PATTERN = re.compile(r"^PT\d{10}HU$")


def new_parcel_id() -> str:
    """Postai stílusú követési azonosító, pl. PT0123456789HU."""
    return f"PT{random.randrange(10**10):010d}HU"


SORTING_CENTERS = [
    "Budapest OLK",
    "Budaörs HUB",
    "Debrecen Feldolgozó",
    "Szeged Feldolgozó",
    "Győr Feldolgozó",
    "Pécs Feldolgozó",
    "Miskolc Feldolgozó",
    "Nyíregyháza Feldolgozó",
]

DESTINATIONS = [
    "Budapest",
    "Debrecen",
    "Szeged",
    "Miskolc",
    "Pécs",
    "Győr",
    "Nyíregyháza",
    "Kecskemét",
    "Székesfehérvár",
    "Szombathely",
    "Albertirsa",
]


class ParcelEvent(BaseModel):
    """Egyetlen esemény a csomag életútján — ez utazik a Kafka topicon.

    Az ``event_id`` az idempotencia kulcsa: újrakézbesítéskor (at-least-once)
    a worker erről ismeri fel, hogy már feldolgozta.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    parcel_id: str
    event_type: EventType
    location: str = ""
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("parcel_id")
    @classmethod
    def _check_parcel_id(cls, value: str) -> str:
        if not PARCEL_ID_PATTERN.match(value):
            raise ValueError(
                f"Érvénytelen csomagazonosító: {value!r} (elvárt formátum: PT + 10 számjegy + HU)"
            )
        return value

    def to_kafka_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
