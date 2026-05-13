"""Circuit breaker for external API calls.

Prevents cascading failures by blocking calls to failing services.
When a service fails repeatedly, the circuit opens and calls are rejected
immediately without attempting the actual request. After a cooldown period,
the circuit transitions to half-open state to test if the service has recovered.

Usage:
    from mandala.core.circuit_breaker import CircuitBreaker

    breaker = CircuitBreaker(
        name="samsara_api",
        failure_threshold=5,
        recovery_timeout=60,
        expected_exception=Exception,
    )

    async with breaker:
        result = await samsara_client.get_truck(truck_id)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# Rust acceleration for Circuit Breaker state machine logic
try:
    from mandala_rust_ext import CircuitBreaker as RustCircuitBreaker

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = auto()  # Normal operation, requests allowed
    OPEN = auto()  # Circuit is open, requests rejected
    HALF_OPEN = auto()  # Testing if service has recovered


# Alias for backward compatibility with tests
CircuitBreakerState = CircuitState


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    name: str
    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: float = 60.0  # Seconds to wait before half-open
    expected_exception: type[Exception] = Exception  # Exception type to catch
    success_threshold: int = 2  # Successes needed to close circuit
    half_open_max_calls: int = 3  # Max calls in half-open state (for test compatibility)


class CircuitBreaker:
    """Circuit breaker for external API calls."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
        success_threshold: int = 2,
        half_open_max_calls: int = 3,
    ) -> None:
        self._config = CircuitBreakerConfig(
            name=name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            success_threshold=success_threshold,
            half_open_max_calls=half_open_max_calls,
        )
        self._lock = asyncio.Lock()

        # Use Rust state machine if available (non-blocking, preserves async architecture)
        if _RUST_EXT_AVAILABLE:
            self._rust_breaker = RustCircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
                success_threshold=success_threshold,
            )
        else:
            self._rust_breaker = None
            # Fallback to Python state
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time: float | None = None

    async def __aenter__(self) -> CircuitBreaker:
        """Enter context manager - check circuit state."""
        async with self._lock:
            current_time = time.time()

            if self._rust_breaker:
                # Use Rust state machine logic (non-blocking)
                can_proceed = self._rust_breaker.check_state(current_time)
                if not can_proceed:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self._config.name}' is OPEN. "
                        f"Rejecting request to prevent cascading failure."
                    )
                state_name = self._rust_breaker.get_state_name()
                if state_name == "half_open":
                    log.info(
                        "circuit_breaker.half_open",
                        name=self._config.name,
                        recovery_timeout=self._config.recovery_timeout,
                    )
            else:
                # Fallback to Python logic
                if self._state == CircuitState.OPEN:
                    # Check if recovery timeout has elapsed
                    if (
                        self._last_failure_time
                        and current_time - self._last_failure_time > self._config.recovery_timeout
                    ):
                        self._state = CircuitState.HALF_OPEN
                        self._success_count = 0
                        log.info(
                            "circuit_breaker.half_open",
                            name=self._config.name,
                            recovery_timeout=self._config.recovery_timeout,
                        )
                    else:
                        raise CircuitBreakerOpenError(
                            f"Circuit breaker '{self._config.name}' is OPEN. "
                            f"Rejecting request to prevent cascading failure."
                        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager - record success or failure."""
        async with self._lock:
            current_time = time.time()

            if exc_type is not None and issubclass(exc_type, self._config.expected_exception):
                # Failure occurred
                if self._rust_breaker:
                    self._rust_breaker.record_failure(current_time)
                    state_name = self._rust_breaker.get_state_name()
                    if state_name == "open":
                        log.warning(
                            "circuit_breaker.opened",
                            name=self._config.name,
                        )
                    else:
                        log.warning(
                            "circuit_breaker.failure",
                            name=self._config.name,
                        )
                else:
                    # Fallback to Python logic
                    self._failure_count += 1
                    self._last_failure_time = current_time
                    self._success_count = 0

                    if self._failure_count >= self._config.failure_threshold:
                        self._state = CircuitState.OPEN
                        log.warning(
                            "circuit_breaker.opened",
                            name=self._config.name,
                            failure_count=self._failure_count,
                            threshold=self._config.failure_threshold,
                        )
                    else:
                        log.warning(
                            "circuit_breaker.failure",
                            name=self._config.name,
                            failure_count=self._failure_count,
                            threshold=self._config.failure_threshold,
                        )
            else:
                # Success occurred
                if self._rust_breaker:
                    self._rust_breaker.record_success()
                    state_name = self._rust_breaker.get_state_name()
                    if state_name == "closed":
                        log.info(
                            "circuit_breaker.closed",
                            name=self._config.name,
                        )
                    elif state_name == "half_open":
                        log.info(
                            "circuit_breaker.half_open_success",
                            name=self._config.name,
                        )
                else:
                    # Fallback to Python logic
                    self._failure_count = 0
                    self._success_count += 1

                    if self._state == CircuitState.HALF_OPEN:
                        if self._success_count >= self._config.success_threshold:
                            self._state = CircuitState.CLOSED
                            log.info(
                                "circuit_breaker.closed",
                                name=self._config.name,
                                success_count=self._success_count,
                            )
                        else:
                            log.info(
                                "circuit_breaker.half_open_success",
                                name=self._config.name,
                                success_count=self._success_count,
                                threshold=self._config.success_threshold,
                            )

    def get_state(self) -> CircuitState:
        """Get current circuit state (thread-safe read)."""
        if self._rust_breaker:
            state_name = self._rust_breaker.get_state_name()
            state_map = {"closed": CircuitState.CLOSED, "open": CircuitState.OPEN, "half_open": CircuitState.HALF_OPEN}
            return state_map.get(state_name, CircuitState.CLOSED)
        return self._state

    @property
    def state(self) -> CircuitState:
        """Property for backward compatibility with tests."""
        return self.get_state()

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        if self._rust_breaker:
            return {
                "name": self._config.name,
                "state": self._rust_breaker.get_state_name(),
                "failure_count": self._rust_breaker.failure_count,
                "success_count": self._rust_breaker.success_count,
                "last_failure_time": self._rust_breaker.last_failure_time,
                "failure_threshold": self._config.failure_threshold,
                "recovery_timeout": self._config.recovery_timeout,
            }
        return {
            "name": self._config.name,
            "state": self._state.name,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
            "failure_threshold": self._config.failure_threshold,
            "recovery_timeout": self._config.recovery_timeout,
        }

    # Compatibility properties for tests
    @property
    def _failure_threshold(self) -> int:
        return self._config.failure_threshold

    @property
    def _recovery_timeout(self) -> float:
        return self._config.recovery_timeout

    @property
    def _success_threshold(self) -> int:
        return self._config.success_threshold

    @property
    def _half_open_max_calls(self) -> int:
        return self._config.half_open_max_calls

    async def call(self, func: object, *args: Any, **kwargs: Any) -> Any:
        """Call a function through the circuit breaker.

        Args:
            func: The async function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function (timeout, fallback, max_retries)

        Returns:
            The result of the function call

        Raises:
            CircuitBreakerOpenError: If the circuit is open
            Exception: If the function raises an exception
        """
        timeout = kwargs.pop("timeout", None)
        fallback = kwargs.pop("fallback", None)
        max_retries = kwargs.pop("max_retries", 0)

        async def _attempt():
            async with self:
                if timeout:
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                return await func(*args, **kwargs)

        # Retry logic
        for attempt in range(max_retries + 1):
            try:
                return await _attempt()
            except Exception as exc:
                if attempt == max_retries:
                    if fallback:
                        return await fallback()
                    raise
                await asyncio.sleep(0.1 * (attempt + 1))
        return None  # Explicit return for type checker

    async def check_state(self) -> None:
        """Check and update circuit state based on timeout."""
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and time.time() - self._last_failure_time > self._config.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    log.info(
                        "circuit_breaker.half_open",
                        name=self._config.name,
                        recovery_timeout=self._config.recovery_timeout,
                    )

    def get_metrics(self) -> dict[str, Any]:
        """Get circuit breaker metrics."""
        return {
            "total_calls": self._failure_count + self._success_count,
            "successful_calls": self._success_count,
            "failed_calls": self._failure_count,
            "success_rate": self._success_count / max(1, self._failure_count + self._success_count),
        }

    def set_exception_handler(self, handler: object) -> None:
        """Set custom exception handler."""
        self._exception_handler = handler

    def set_exception_filter(self, filter_func: object) -> None:
        """Set custom exception filter."""
        self._exception_filter = filter_func

    async def reset(self) -> None:
        """Manually reset the circuit breaker to CLOSED state."""
        async with self._lock:
            if self._rust_breaker:
                self._rust_breaker.reset()
            else:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                self._last_failure_time = None
            log.info("circuit_breaker.reset", name=self._config.name)


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and rejects a request."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class CircuitBreakerRegistry:
    """Registry for circuit breakers - allows monitoring and management."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def register(self, breaker: CircuitBreaker) -> None:
        """Register a circuit breaker."""
        async with self._lock:
            self._breakers[breaker._config.name] = breaker
            log.info("circuit_breaker.registered", name=breaker._config.name)

    async def get(self, name: str) -> CircuitBreaker | None:
        """Get a circuit breaker by name."""
        async with self._lock:
            return self._breakers.get(name)

    async def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all registered circuit breakers."""
        async with self._lock:
            return {name: breaker.get_stats() for name, breaker in self._breakers.items()}

    async def reset_all(self) -> None:
        """Reset all circuit breakers to CLOSED state."""
        async with self._lock:
            for breaker in self._breakers.values():
                await breaker.reset()
            log.info("circuit_breaker.all_reset", count=len(self._breakers))


# Global registry instance
_global_registry = CircuitBreakerRegistry()


def get_global_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry."""
    return _global_registry
