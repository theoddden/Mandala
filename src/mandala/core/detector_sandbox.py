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
    ) -> None:
        self._detector_func = detector_func
        self._detector_name = detector_name
        self._timeout = timeout_seconds
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout,
        )

    async def execute(
        self, event: MandalaEvent, state: StateStore, redis: object
    ) -> list[MandalaEvent]:
        """Execute detector with timeout and circuit breaker protection."""
        # Check circuit breaker
        if self._circuit_breaker.is_open():
            log.warning(
                "detector.circuit_breaker_open",
                detector=self._detector_name,
                event_id=event.id,
            )
            return []

        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                self._detector_func(event, state, redis),
                timeout=self._timeout,
            )
            
            # Record success
            self._circuit_breaker.record_success()
            return result

        except TimeoutError:
            log.error(
                "detector.timeout",
                detector=self._detector_name,
                event_id=event.id,
                timeout_seconds=self._timeout,
            )
            self._circuit_breaker.record_failure()
            return []

        except Exception as exc:
            log.exception(
                "detector.execution_failed",
                detector=self._detector_name,
                event_id=event.id,
                error=str(exc),
            )
            self._circuit_breaker.record_failure()
            raise


class DetectorSandboxPool:
    """Manages sandboxed detectors with per-detector configuration."""

    def __init__(self, detectors: list[Callable]) -> None:
        self._sandboxes: dict[str, DetectorSandbox] = {}
        self._detectors = detectors

        # Create sandboxes for each detector
        for detector in detectors:
            detector_name = detector.__name__
            
            # Configure timeouts based on detector type
            # ML inference and external API calls get longer timeouts
            if any(keyword in detector_name.lower() for keyword in ["ml", "model", "predict", "fmcsa", "vizion"]):
                timeout = 60.0
            else:
                timeout = 10.0
            
            self._sandboxes[detector_name] = DetectorSandbox(
                detector_func=detector,
                detector_name=detector_name,
                timeout_seconds=timeout,
                circuit_breaker_threshold=5,
                circuit_breaker_timeout=60.0,
            )

    async def execute_all(
        self, event: MandalaEvent, state: StateStore, redis: object
    ) -> list[MandalaEvent]:
        """Execute all detectors in parallel with sandbox protection."""
        tasks = [
            sandbox.execute(event, state, redis)
            for sandbox in self._sandboxes.values()
        ]

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
        return {
            name: {
                "is_open": sandbox._circuit_breaker.is_open(),
                "failure_count": sandbox._circuit_breaker._failure_count,
                "last_failure_time": sandbox._circuit_breaker._last_failure_time,
            }
            for name, sandbox in self._sandboxes.items()
        }
