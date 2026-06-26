"""Mandala FastAPI app — webhook ingest only.

Trimmed for one-person ops: a single process, single Redis stream, optional
connectors. Heavy work (alerts, projection, MCP) runs in the ``mandala worker``
process which reads the same stream.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as redis
import structlog
from fastapi import FastAPI, HTTPException, Request, status

from mandala import __version__
from mandala.core.adaptive_backpressure import AdaptiveBackpressure
from mandala.core.backpressure import BackpressureMiddleware as SimpleBackpressureMiddleware
from mandala.core.bus import RedisStreamsBus
from mandala.core.compliance.access_logger import AccessLogMiddleware
from mandala.core.compliance.data_residency import DataResidencyMiddleware
from mandala.core.events.envelope import SCHEMA_VERSION
from mandala.core.events.idempotency import RedisIdempotencyStore
from mandala.core.rate_limit import RateLimitMiddleware
from mandala.settings import Settings, get_settings

log = structlog.get_logger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    s: Settings = app.state.settings
    app.state.redis = redis.from_url(s.redis_url, decode_responses=False)
    app.state.bus = RedisStreamsBus(app.state.redis)
    app.state.idempotency = RedisIdempotencyStore(app.state.redis)

    # Initialize adaptive backpressure if enabled
    if s.adaptive_backpressure_enabled:
        app.state.adaptive_backpressure = AdaptiveBackpressure(app.state.redis)
        log.info("mandala.adaptive_backpressure_enabled")
    else:
        app.state.adaptive_backpressure = None

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

    # Add adaptive backpressure middleware if enabled
    if s.adaptive_backpressure_enabled:
        # Use a custom middleware that will use the AdaptiveBackpressure instance
        # The middleware is added after lifespan so it can access app.state.adaptive_backpressure
        from starlette.middleware.base import BaseHTTPMiddleware

        class AdaptiveBackpressureMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                # Only check backpressure for webhook endpoints
                if (
                    request.url.path.startswith("/webhooks/")
                    and hasattr(request.app.state, "adaptive_backpressure")
                    and request.app.state.adaptive_backpressure
                ):
                    should_accept, reason = await request.app.state.adaptive_backpressure.should_accept_new_event()
                    if not should_accept:
                        from fastapi import Response, status

                        return Response(
                            content=f"System degraded: {reason}",
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        )
                return await call_next(request)

        app.add_middleware(AdaptiveBackpressureMiddleware)
    else:
        # Fall back to simple stream-length based backpressure
        app.add_middleware(SimpleBackpressureMiddleware)

    # Add rate limiting middleware to prevent abuse
    app.add_middleware(RateLimitMiddleware)

    # Add compliance middleware (access logging and data residency)
    if s.audit_access_log_enabled:
        app.add_middleware(AccessLogMiddleware, enabled=True)
        log.info("mandala.audit_access_log_enabled")

    if s.data_residency_enabled:
        app.add_middleware(DataResidencyMiddleware, allowed_regions=s.data_residency_allowed_regions)
        log.info("mandala.data_residency_enabled", regions=s.data_residency_allowed_regions)

    @app.get("/healthz", tags=["meta"])
    async def healthz() -> dict[str, str]:
        """Basic health check - always returns ok if process is running."""
        return {"status": "ok"}

    @app.get("/readyz", tags=["meta"])
    async def readyz() -> dict[str, Any]:
        """Readiness check - verifies Redis connectivity and stream health."""
        health_status: dict[str, Any] = {"status": "ready", "checks": {}}

        # Check Redis connectivity
        try:
            await app.state.redis.ping()
            health_status["checks"]["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            health_status["status"] = "not_ready"
            health_status["checks"]["redis"] = f"failed: {str(exc)}"
            return health_status

        # Check stream exists and is writable
        try:
            s = app.state.settings
            # Try to read stream info
            info = await app.state.redis.xinfo_stream(s.stream_inbound)  # type: ignore[attr-defined]
            health_status["checks"]["stream"] = "ok"
            health_status["checks"]["stream_length"] = info.get(b"length", 0)
            health_status["checks"]["stream_groups"] = info.get(b"groups", 0)
        except Exception as exc:  # noqa: BLE001
            exc_str = str(exc)
            if "no such key" in exc_str.lower() or "ERR no such key" in exc_str:
                # Stream doesn't exist yet — normal on first start before any events arrive
                health_status["checks"]["stream"] = "pending_first_event"
            else:
                health_status["status"] = "not_ready"
                health_status["checks"]["stream"] = f"failed: {exc_str}"
                return health_status

        return health_status

    @app.get("/version", tags=["meta"])
    async def version() -> dict[str, str]:
        return {"mandala": __version__, "schema": SCHEMA_VERSION}

    from mandala.core.events.envelope import MandalaEvent

    @app.post("/events", status_code=status.HTTP_202_ACCEPTED, tags=["ingest"])
    async def ingest_event(request: Request) -> dict[str, str]:
        """Direct event ingest endpoint.

        Accepts a canonical MandalaEvent JSON body and publishes it to the
        inbound stream. Useful for custom integrations that don't map to an
        existing webhook connector.
        """
        body = await request.body()
        try:
            event = MandalaEvent.model_validate_json(body)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid MandalaEvent: {exc}",
            ) from exc

        s = request.app.state.settings
        bus: RedisStreamsBus = request.app.state.bus
        msg_id = await bus.publish(s.stream_inbound, event)
        return {"msg_id": msg_id or "", "status": "duplicate" if not msg_id else "accepted"}

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

        app.include_router(cargowise_router, prefix="/webhooks/cargowise", tags=["cargowise"])
    except ImportError:
        log.info("mandala.connector.cargowise.disabled")

    return app


app = create_app()
