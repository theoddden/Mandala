"""Access logging middleware for audit compliance.

Logs all /events POST requests to a dedicated Redis stream for
audit trail purposes. Separate from main event stream to avoid
pollution.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from starlette.types import ASGIApp

log = structlog.get_logger(__name__)


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Middleware for access logging.

    Logs all /events POST requests to Redis stream for audit trail.
    Redis is fetched lazily from request.app.state to avoid capturing
    a None reference before the lifespan startup hook runs.
    """

    def __init__(self, app: ASGIApp, enabled: bool = True) -> None:
        """Initialize access logging middleware.

        Args:
            app: The ASGI app to wrap
            enabled: Whether access logging is active
        """
        super().__init__(app)
        self._enabled = enabled
        self._stream_name = "mandala:audit:access"

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        """Process request and log access.

        Args:
            request: The incoming request
            call_next: The next middleware/route handler

        Returns:
            Response from next handler
        """
        if not self._enabled:
            return await call_next(request)

        # Only log POST requests to /events or webhook endpoints
        if request.method != "POST":
            return await call_next(request)

        path = request.url.path
        if not (path == "/events" or path.startswith("/webhooks/")):
            return await call_next(request)

        # Fetch Redis lazily from app.state (set by lifespan startup)
        redis = getattr(getattr(request.app, "state", None), "redis", None)
        if redis is None:
            return await call_next(request)

        # Capture request info
        client_host = request.client.host if request.client else "unknown"
        event_type = None
        subject = None

        try:
            body = await request.json()
            event_type = body.get("type")
            subject = body.get("subject")
        except Exception:
            log.debug("access_logger.non_json_body", path=path)

        # Log to Redis stream (fire-and-forget)
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "ip": client_host,
            "path": path,
            "event_type": event_type,
            "subject": subject,
            "user_agent": request.headers.get("user-agent", "unknown"),
        }

        try:
            await redis.xadd(self._stream_name, log_entry)  # type: ignore[attr-defined]
            log.debug(
                "access.logged",
                ip=client_host,
                path=path,
                event_type=event_type,
            )
        except Exception as exc:
            log.exception("access.logging_failed", error=str(exc))

        return await call_next(request)
