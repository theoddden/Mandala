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

log = structlog.get_logger(__name__)


class AccessLogMiddleware:
    """Middleware for access logging.

    Logs all /events POST requests to Redis stream for audit trail.
    """

    def __init__(self, redis_client: Any, enabled: bool = True) -> None:
        """Initialize access logging middleware.

        Args:
            redis_client: Redis client for logging
            enabled: Whether access logging is active
        """
        self._redis = redis_client
        self._enabled = enabled
        self._stream_name = "mandala:audit:access"

    async def __call__(self, request: Request, call_next) -> Any:
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

        # Capture request info
        client_host = request.client.host if request.client else "unknown"
        event_type = None
        subject = None

        try:
            body = await request.json()
            event_type = body.get("type")
            subject = body.get("subject")
        except Exception:
            # If we can't parse JSON, still log the attempt
            pass

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
            await self._redis.xadd(self._stream_name, log_entry)  # type: ignore[attr-defined]
            log.debug(
                "access.logged",
                ip=client_host,
                path=path,
                event_type=event_type,
            )
        except Exception as exc:
            log.exception("access.logging_failed", error=str(exc))

        return await call_next(request)
