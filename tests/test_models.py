"""Az állapotgép és a domain modellek tesztjei."""
import pytest
from pydantic import ValidationError

from parcel_tracker.models import (
    PARCEL_ID_PATTERN,
    EventType,
    InvalidTransitionError,
    ParcelEvent,
    ParcelStatus,
    new_parcel_id,
    validate_transition,
)


def test_parcel_id_format():
    for _ in range(50):
        assert PARCEL_ID_PATTERN.match(new_parcel_id())


def test_happy_path_lifecycle():
    """Feladástól kézbesítésig minden átmenet érvényes."""
    status = validate_transition(None, EventType.FELVETEL)
    assert status == ParcelStatus.FELVEVE
    status = validate_transition(status, EventType.FELDOLGOZAS)
    status = validate_transition(status, EventType.FELDOLGOZAS)  # több központ is lehet
    status = validate_transition(status, EventType.SZALLITAS)
    status = validate_transition(status, EventType.KEZBESITES_INDITVA)
    status = validate_transition(status, EventType.KEZBESITVE)
    assert status == ParcelStatus.KEZBESITVE


def test_failed_delivery_retry():
    status = ParcelStatus.KEZBESITES_ALATT
    status = validate_transition(status, EventType.SIKERTELEN_KEZBESITES)
    assert status == ParcelStatus.SIKERTELEN_KEZBESITES
    status = validate_transition(status, EventType.KEZBESITES_INDITVA)
    status = validate_transition(status, EventType.KEZBESITVE)
    assert status == ParcelStatus.KEZBESITVE


def test_invalid_transition_rejected():
    with pytest.raises(InvalidTransitionError):
        validate_transition(ParcelStatus.FELVEVE, EventType.KEZBESITVE)


def test_terminal_state_rejects_everything():
    for event_type in EventType:
        with pytest.raises(InvalidTransitionError):
            validate_transition(ParcelStatus.KEZBESITVE, event_type)


def test_unknown_parcel_only_accepts_felvetel():
    with pytest.raises(InvalidTransitionError):
        validate_transition(None, EventType.SZALLITAS)


def test_event_validation_rejects_bad_parcel_id():
    with pytest.raises(ValidationError):
        ParcelEvent(parcel_id="ROSSZ-ID", event_type=EventType.FELVETEL)


def test_event_roundtrip_serialization():
    event = ParcelEvent(
        parcel_id=new_parcel_id(),
        event_type=EventType.FELVETEL,
        location="Budapest OLK",
        payload={"destination": "Szeged", "weight_g": 1200},
    )
    restored = ParcelEvent.model_validate_json(event.to_kafka_bytes())
    assert restored == event
