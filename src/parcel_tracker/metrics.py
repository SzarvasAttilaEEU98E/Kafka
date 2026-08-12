"""Prometheus metrikák — egy helyen, hogy a nevek konzisztensek maradjanak.

A kube-prometheus-stack ServiceMonitorokon keresztül kaparja le őket, a
Grafana dashboard (helm/parcel-tracker/templates/grafana-dashboard.yaml)
ezekre a nevekre épül.
"""
from prometheus_client import Counter, Gauge, Histogram

EVENTS_PRODUCED = Counter(
    "parcel_events_produced_total",
    "Kafkába beküldött események száma",
    ["event_type"],
)

EVENTS_CONSUMED = Counter(
    "parcel_events_consumed_total",
    "Worker által feldolgozott események (result: applied|duplicate|dlq)",
    ["event_type", "result"],
)

DLQ_EVENTS = Counter(
    "parcel_dlq_total",
    "Dead letter queue-ba került üzenetek",
    ["reason"],
)

PROCESSING_TIME = Histogram(
    "parcel_event_processing_seconds",
    "Egy esemény feldolgozási ideje a workerben",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "API HTTP kérések",
    ["method", "path", "status"],
)

HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "API HTTP válaszidő",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

PARCELS_BY_STATUS = Gauge(
    "parcel_status_count",
    "Csomagok száma státusz szerint (Redis materializált nézetből)",
    ["status"],
)

PARCELS_TOTAL = Gauge(
    "parcels_total",
    "Összes eddig felvett csomag",
)

WORKER_READY = Gauge(
    "worker_ready",
    "A worker fut és fogyaszt (1=igen)",
)
