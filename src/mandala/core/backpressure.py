"""Backpressure middleware for webhook endpoints.

Rejects new events when the system is overloaded to prevent cascading failures.
Checks stream length and returns configured HTTP status code when threshold exceeded.
"""
from __future__ import annotations

from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from mandala.settings import get_settings


class BackpressureMiddleware(BaseHTTPMiddleware):
    """Middleware that rejects requests when system is overloaded."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Check backpressure before processing request."""
        s = get_settings()
        
        if not s.backpressure_enabled:
            return await call_next(request)
        
        # Only check backpressure for webhook endpoints
        if not request.url.path.startswith("/webhooks/"):
            return await call_next(request)
        
        try:
            # Get stream length from Redis
            redis = request.app.state.redis
            stream_length = await redis.xlen(s.stream_inbound)  # type: ignore[attr-defined]
            
            if stream_length >= s.backpressure_threshold:
                # System is overloaded, reject request
                import structlog
                log = structlog.get_logger(__name__)
                log.warning(
                    "backpressure.active",
                    stream=s.stream_inbound,
                    stream_length=stream_length,
                    threshold=s.backpressure_threshold,
                    path=request.url.path,
                )
                
                return Response(
                    content=f"System overloaded. Stream length ({stream_length}) exceeds threshold ({s.backpressure_threshold}). Please retry later.",
                    status_code=s.backpressure_response_code,
                )
            
            return await call_next(request)
            
        except Exception:
            # If backpressure check fails, allow request to proceed
            # (fail-open to prevent blocking all traffic on check failure)
            return await call_next(request)
