"""Központi konfiguráció.

Minden beállítás környezeti változóból érkezik (12-factor elv): ugyanaz a
konténerkép fut lokálisan, docker compose-ban és Kubernetesben, csak a
környezet más. A változónevek megegyeznek a mezőnevek nagybetűs alakjával
(pl. ``KAFKA_BOOTSTRAP_SERVERS``).
"""
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = "localhost:29092"  # docker compose host-listener
    kafka_topic: str = "parcel.events"
    kafka_dlq_topic: str = "parcel.events.dlq"
    kafka_consumer_group: str = "parcel-worker"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # HTTP portok
    api_port: int = 8000
    metrics_port: int = 8001  # worker Prometheus endpointja

    # Szimulátor
    api_base_url: str = "http://localhost:8000"
    sim_parcels_per_minute: float = 10.0
    sim_chaos_percent: float = 3.0  # ennyi %-ban küld szándékosan hibás eseményt (DLQ demó)

    # Viselkedés
    stats_cache_ttl_seconds: int = 5  # cache-aside TTL az összesítő statisztikára
    dedup_ttl_seconds: int = 86400  # idempotencia-kulcsok élettartama
    # A csomagadatok (hash + history) élettartama: demó adat, nem nőhet a
    # végtelenségig — a Redis maxmemory+noeviction párossal ez kötelező.
    parcel_ttl_seconds: int = 259200  # 3 nap
    log_level: str = "INFO"

    model_config = {"case_sensitive": False}


@lru_cache
def get_settings() -> Settings:
    return Settings()
