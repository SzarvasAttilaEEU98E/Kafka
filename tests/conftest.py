"""Közös fixture-ök: fakeredis-alapú store és Kafka-mentes FastAPI app."""
from typing import List

import fakeredis.aioredis
import pytest

from parcel_tracker.models import ParcelEvent
from parcel_tracker.redis_store import RedisStore


class FakeProducer:
    """A Kafka producer helyettesítője: csak feljegyzi, mit küldtünk volna."""

    def __init__(self):
        self.sent: List[dict] = []

    async def send_and_wait(self, topic, value=None, key=None):
        self.sent.append({"topic": topic, "value": value, "key": key})

    async def stop(self):
        pass


@pytest.fixture
async def redis_client():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def store(redis_client):
    return RedisStore(redis_client, dedup_ttl_seconds=3600, cache_ttl_seconds=5)


@pytest.fixture
def fake_producer():
    return FakeProducer()


def sent_events(producer: FakeProducer) -> List[ParcelEvent]:
    return [ParcelEvent.model_validate_json(item["value"]) for item in producer.sent]
