"""Rate limiting middleware for webhook endpoints.

Token bucket algorithm to prevent abuse and protect against webhook floods.
Limits requests per IP address with configurable rate and burst size.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from mandala.settings import get_settings


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, burst: int) -> None:
        self._rate = rate  # tokens per second
        self._burst = burst  # bucket capacity
        self._tokens = float(burst)
        self._last_update = 0.0

    def consume(self, tokens: float = 1.0) -> bool:
        """Consume tokens from the bucket.

        Returns:
            True if tokens were available, False otherwise
        """
        import time

        now = time.time()

        # Add tokens based on elapsed time
        if self._last_update > 0:
            elapsed = now - self._last_update
            self._tokens = min(self._tokens + elapsed * self._rate, self._burst)

        self._last_update = now

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def reset(self) -> None:
        """Reset the token bucket."""
        self._tokens = float(self._burst)
        self._last_update = 0.0


class RateLimiter:
    """IP-based rate limiter using token bucket algorithm."""

    def __init__(self, rate_per_minute: int, burst_size: int) -> None:
        self._rate = rate_per_minute / 60.0  # convert to tokens per second
        self._burst = burst_size
        self._buckets: dict[str, TokenBucket] = defaultdict(lambda: TokenBucket(self._rate, self._burst))

    def is_allowed(self, ip: str) -> bool:
        """Check if request from IP is allowed.

        Args:
            ip: IP address

        Returns:
            True if request is allowed, False otherwise
        """
        bucket = self._buckets[ip]
        return bucket.consume()

    def reset_ip(self, ip: str) -> None:
        """Reset rate limit for a specific IP."""
        if ip in self._buckets:
            del self._buckets[ip]

    def get_stats(self) -> dict[str, int]:
        """Get rate limiter statistics.

        Returns:
            Dictionary with stats (total buckets, etc.)
        """
        return {
            "total_ips": len(self._buckets),
            "rate_per_minute": int(self._rate * 60),
            "burst_size": self._burst,
        }


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces rate limiting on webhook endpoints."""

    def __init__(self, app) -> None:
        super().__init__(app)
        self._limiter: RateLimiter | None = None

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Check rate limit before processing request."""
        s = get_settings()

        if not s.rate_limit_enabled:
            return await call_next(request)

        # Only rate limit webhook endpoints
        if not request.url.path.startswith("/webhooks/"):
            return await call_next(request)

        # Initialize limiter if not already done
        if self._limiter is None:
            self._limiter = RateLimiter(
                rate_per_minute=s.rate_limit_requests_per_minute,
                burst_size=s.rate_limit_burst_size,
            )

        # Get client IP
        ip = self._get_client_ip(request)

        # Check rate limit
        if not self._limiter.is_allowed(ip):
            import structlog

            log = structlog.get_logger(__name__)
            log.warning(
                "rate_limit.exceeded",
                ip=ip,
                path=request.url.path,
            )

            from fastapi import status

            return Response(
                content=f"Rate limit exceeded for IP {ip}. Maximum {s.rate_limit_requests_per_minute} requests per minute allowed.",
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            )

        return await call_next(request)

    def _get_client_ip(self, request: Request) -> str:
        """Get client IP address from request.

        Checks X-Forwarded-For header for proxied requests.
        """
        # Check X-Forwarded-For header (for proxied requests)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fall back to direct connection
        if request.client:
            return request.client.host

        return "unknown"
