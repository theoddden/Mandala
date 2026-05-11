"""Connection resilience utilities for automatic retry with exponential backoff.

Provides decorators and utilities for adding automatic retry logic to Redis
operations and external API calls with configurable backoff strategies.
"""
from __future__ import annotations

import asyncio
import random
import time
from functools import wraps
from typing import Any, Callable, Type

import structlog

log = structlog.get_logger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 10.0,
    exponential: bool = True,
    jitter: bool = True,
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,),
    on_retry: Callable[[int, Exception], None] | None = None,
) -> Callable:
    """Decorator for automatic retry with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential: Use exponential backoff (2^n) or linear (n * base_delay)
        jitter: Add random jitter to delay to prevent thundering herd
        retryable_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called on each retry attempt
        
    Example:
        @retry_with_backoff(max_retries=3, base_delay=0.1)
        async def redis_operation():
            await redis.ping()
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exception = exc
                    
                    if attempt >= max_retries:
                        # Exhausted retries, re-raise
                        log.error(
                            "retry.exhausted",
                            function=func.__name__,
                            attempt=attempt,
                            max_retries=max_retries,
                            error=str(exc),
                        )
                        raise
                    
                    # Calculate delay
                    if exponential:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                    else:
                        delay = min(base_delay * (attempt + 1), max_delay)
                    
                    # Add jitter if enabled
                    if jitter:
                        jitter_amount = delay * 0.2 * (random.random() * 2 - 1)
                        delay = max(delay + jitter_amount, 0.01)
                    
                    # Call on_retry callback if provided
                    if on_retry:
                        on_retry(attempt, exc)
                    
                    log.warning(
                        "retry.attempt",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay_sec=delay,
                        error=str(exc),
                    )
                    
                    await asyncio.sleep(delay)
            
            # Should never reach here, but type checker needs it
            if last_exception:
                raise last_exception
            raise RuntimeError("Retry logic error")
        
        return async_wrapper
    
    return decorator


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and rejects a request."""
    pass


class ResilientRedis:
    """Redis client wrapper with automatic retry and connection resilience."""
    
    def __init__(self, redis_client: Any, max_retries: int = 3) -> None:
        self._redis = redis_client
        self._max_retries = max_retries
    
    async def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to underlying Redis client with retry."""
        attr = getattr(self._redis, name)
        
        if not callable(attr):
            return attr
        
        @retry_with_backoff(
            max_retries=self._max_retries,
            base_delay=0.1,
            max_delay=5.0,
            retryable_exceptions=(ConnectionError, TimeoutError, OSError),
        )
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            return await attr(*args, **kwargs)
        
        return wrapped
    
    async def aclose(self) -> None:
        """Close the underlying Redis connection."""
        if hasattr(self._redis, "aclose"):
            await self._redis.aclose()
        elif hasattr(self._redis, "close"):
            await self._redis.close()


class HealthCheck:
    """Periodic health check for external dependencies."""
    
    def __init__(
        self,
        check_func: Callable[[], Any],
        interval: float = 30.0,
        timeout: float = 5.0,
    ) -> None:
        self._check_func = check_func
        self._interval = interval
        self._timeout = timeout
        self._last_check: float | None = None
        self._last_status: bool = False
        self._last_error: str | None = None
        self._task: asyncio.Task[None] | None = None
    
    async def _run_checks(self) -> None:
        """Run periodic health checks."""
        while True:
            try:
                await asyncio.wait_for(self._check_func(), timeout=self._timeout)
                self._last_status = True
                self._last_error = None
            except asyncio.TimeoutError:
                self._last_status = False
                self._last_error = "Health check timed out"
                log.warning("health_check.timeout")
            except Exception as exc:
                self._last_status = False
                self._last_error = str(exc)
                log.warning("health_check.failed", error=str(exc))
            
            self._last_check = time.time()
            await asyncio.sleep(self._interval)
    
    async def start(self) -> None:
        """Start periodic health checks."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_checks())
            log.info("health_check.started", interval=self._interval)
    
    async def stop(self) -> None:
        """Stop periodic health checks."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            log.info("health_check.stopped")
    
    def get_status(self) -> dict[str, Any]:
        """Get current health check status."""
        return {
            "healthy": self._last_status,
            "last_check": self._last_check,
            "last_error": self._last_error,
            "interval": self._interval,
        }
