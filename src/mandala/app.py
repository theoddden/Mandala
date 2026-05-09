"""Mandala FastAPI app — webhook ingest only.

Trimmed for one-person ops: a single process, single Redis stream, optional
connectors. Heavy work (alerts, projection, MCP) runs in the ``mandala worker``
process which reads the same stream.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import redis.asyncio as redis
import structlog
from fastapi import FastAPI

from mandala import __version__
from mandala.core.bus import RedisStreamsBus
from mandala.core.events.envelope import SCHEMA_VERSION
from mandala.core.events.idempotency import RedisIdempotencyStore
from mandala.settings import Settings, get_settings

log = structlog.get_logger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    s: Settings = app.state.settings
    app.state.redis = redis.from_url(s.redis_url, decode_responses=False)
    app.state.bus = RedisStreamsBus(app.state.redis)
    app.state.idempotency = RedisIdempotencyStore(app.state.redis)
    log.info("mandala.startup", redis=s.redis_url)
    try:
        yield
    finally:
        await app.state.redis.aclose()


def create_app(settings: Settings | None = None) -> FastAPI:
    s = settings or get_settings()
    app = FastAPI(
        title="Mandala",
        version=__version__,
        description="The bridge between the wheel and the plane.",
        lifespan=_lifespan,
    )
    app.state.settings = s

    @app.get("/healthz", tags=["meta"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["meta"])
    async def readyz() -> dict[str, str]:
        try:
            await app.state.redis.ping()
        except Exception as exc:  # noqa: BLE001
            return {"status": "degraded", "error": str(exc)}
        return {"status": "ready"}

    @app.get("/version", tags=["meta"])
    async def version() -> dict[str, str]:
        return {"mandala": __version__, "schema": SCHEMA_VERSION}

    # Connectors register their own webhook routers. Each is optional —
    # Mandala must run usefully with only Samsara configured.
    from mandala.connectors.samsara.webhook import router as samsara_router

    app.include_router(samsara_router, prefix="/webhooks/samsara", tags=["samsara"])

    try:
        from mandala.connectors.descartes.macropoint.webhook import (
            router as macropoint_router,
        )

        app.include_router(
            macropoint_router,
            prefix="/webhooks/descartes/macropoint",
            tags=["descartes-macropoint"],
        )
    except ImportError:
        log.info("mandala.connector.macropoint.disabled")

    try:
        from mandala.connectors.cargowise.webhook import router as cargowise_router

        app.include_router(
            cargowise_router, prefix="/webhooks/cargowise", tags=["cargowise"]
        )
    except ImportError:
        log.info("mandala.connector.cargowise.disabled")

    return app


app = create_app()
