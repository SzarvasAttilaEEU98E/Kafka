"""FastAPI alkalmazás összeállítása.

Életciklus (lifespan): induláskor Redis kapcsolat + Kafka producer (retryvel,
mert K8s-ben a broker később is indulhat), leálláskor rendezett lezárás.
A /healthz és /readyz végpontok a Kubernetes liveness/readiness probe-oknak
készültek — a kettő közti különbség tananyag!
"""
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .. import __version__
from ..config import get_settings
from ..kafka_client import create_producer
from ..metrics import HTTP_LATENCY, HTTP_REQUESTS
from ..redis_store import RedisStore
from .routes import router

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app.state.store = RedisStore.from_url(
        settings.redis_url,
        dedup_ttl_seconds=settings.dedup_ttl_seconds,
        cache_ttl_seconds=settings.stats_cache_ttl_seconds,
        parcel_ttl_seconds=settings.parcel_ttl_seconds,
    )
    app.state.producer = await create_producer()
    app.state.ready = True
    log.info("API kész (v%s)", __version__)
    yield
    app.state.ready = False
    await app.state.producer.stop()
    await app.state.store.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Posta Csomagkövető",
        description="Kafka + Redis event-driven demó — oktatási projekt",
        version=__version__,
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def observe_requests(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        # A sablonos útvonalat mérjük ({parcel_id}), nem a konkrét URL-t,
        # különben felrobbanna a metrika-kardinalitás. Az ismeretlen (404-es)
        # útvonalak közös vödörbe mennek — publikus URL-en a botok
        # végtelen sok egyedi útvonalat generálnának.
        path = getattr(route, "path", None) or "unmatched"
        HTTP_REQUESTS.labels(request.method, path, str(response.status_code)).inc()
        HTTP_LATENCY.labels(request.method, path).observe(time.perf_counter() - started)
        return response

    app.include_router(router, prefix="/api")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> PlainTextResponse:
        # Liveness: "él-e a processz?" — mindig 200, ha ide eljut a kérés.
        return PlainTextResponse("ok")

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> PlainTextResponse:
        # Readiness: "fogadhat-e forgalmat?" — függőségeket is nézünk.
        if not getattr(request.app.state, "ready", False):
            return PlainTextResponse("indulás alatt", status_code=503)
        try:
            await request.app.state.store.ping()
        except Exception:  # noqa: BLE001
            return PlainTextResponse("redis nem érhető el", status_code=503)
        return PlainTextResponse("ok")

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
