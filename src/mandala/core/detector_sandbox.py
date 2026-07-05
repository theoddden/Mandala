"""Detector sandbox with timeout and circuit breaker protection.

Prevents buggy detectors (infinite loops, slow API calls) from blocking
the entire worker process.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import structlog

from mandala.core.circuit_breaker import CircuitBreaker
from mandala.core.events.envelope import MandalaEvent
from mandala.core.state import StateStore
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class DetectorSandbox:
    """Wraps detectors with timeout and circuit breaker protection."""

    def __init__(
        self,
        detector_func: Callable,
        detector_name: str,
        timeout_seconds: float = 30.0,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
        circuit_breaker_name: str | None = None,
    ) -> None:
        self._detector_func = detector_func
        self._detector_name = detector_name
        self._timeout = timeout_seconds
        self._circuit_breaker = CircuitBreaker(
            name=circuit_breaker_name or detector_name,
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout,
        )

    async def execute(self, event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
        """Execute detector with timeout and circuit breaker protection."""
        from mandala.core.circuit_breaker import CircuitBreakerOpenError

        try:
            async with self._circuit_breaker:
                result = await asyncio.wait_for(
                    self._detector_func(event, state, redis),
                    timeout=self._timeout,
                )
                return result or []
        except CircuitBreakerOpenError:
            log.warning(
                "detector.circuit_breaker_open",
                detector=self._detector_name,
                event_id=event.id,
            )
            return []
        except TimeoutError:
            log.error(
                "detector.timeout",
                detector=self._detector_name,
                event_id=event.id,
                timeout_seconds=self._timeout,
            )
            return []
        except Exception as exc:
            log.exception(
                "detector.execution_failed",
                detector=self._detector_name,
                event_id=event.id,
                error=str(exc),
            )
            raise


class DetectorSandboxPool:
    """Manages sandboxed detectors with per-detector configuration."""

    def __init__(self, detectors: list[Callable]) -> None:
        self._sandboxes: dict[str, DetectorSandbox] = {}
        self._detectors = detectors

        # Create sandboxes for each detector
        s = get_settings()
        for idx, detector in enumerate(detectors):
            detector_name = getattr(detector, "__name__", type(detector).__name__)

            # Use a unique key to prevent name collisions between instances of the same class
            # Format: detector_name_<id> or detector_name_<index> if no __name__
            unique_key = f"{detector_name}_{id(detector)}"

            # ML inference and external API calls get double the configured timeout
            if any(keyword in detector_name.lower() for keyword in ["ml", "model", "predict", "fmcsa", "vizion"]):
                timeout = min(s.detector_timeout_seconds * 2, 300.0)
            else:
                timeout = s.detector_timeout_seconds

            self._sandboxes[unique_key] = DetectorSandbox(
                detector_func=detector,
                detector_name=detector_name,
                timeout_seconds=timeout,
                circuit_breaker_threshold=5,
                circuit_breaker_timeout=60.0,
                circuit_breaker_name=unique_key,
            )

    async def execute_all(self, event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
        """Execute all detectors in parallel with sandbox protection."""
        tasks = [sandbox.execute(event, state, redis) for sandbox in self._sandboxes.values()]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results and filter exceptions
        all_events = []
        for result in results:
            if isinstance(result, Exception):
                log.exception("detector.unexpected_error", error=str(result))
            elif isinstance(result, list):
                all_events.extend(result)

        return all_events

    def get_circuit_breaker_status(self) -> dict[str, dict]:
        """Get circuit breaker status for all detectors."""
        from mandala.core.circuit_breaker import CircuitState

        return {
            name: {
                **sandbox._circuit_breaker.get_stats(),
                "is_open": sandbox._circuit_breaker.get_state() == CircuitState.OPEN,
            }
            for name, sandbox in self._sandboxes.items()
        }
