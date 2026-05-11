"""Resource-aware backpressure based on system health.

Adapts batch sizing and processing rate based on Redis latency, memory usage,
and CPU load. Rejects ingestion when system is degraded.
"""
from __future__ import annotations

import asyncio
import psutil
from datetime import datetime, timezone
from typing import Any

import structlog

from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class AdaptiveBackpressure:
    """Monitors system health and adapts processing accordingly."""

    def __init__(self, redis: "object") -> None:
        self._redis = redis
        self._settings = get_settings()
        
        # Health thresholds
        self._redis_latency_threshold_ms = 100.0
        self._memory_threshold_percent = 80.0
        self._cpu_threshold_percent = 80.0
        
        # Adaptive batch size
        self._base_batch_size = self._settings.stream_batch_size
        self._current_batch_size = self._base_batch_size
        self._min_batch_size = 1
        self._max_batch_size = 1000
        
        # Health history for trend detection
        self._health_history: list[dict] = []
        self._max_history = 10

    async def check_health(self) -> dict[str, Any]:
        """Check current system health.

        Returns:
            Health status with metrics and recommendations
        """
        health = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "redis_latency_ms": await self._check_redis_latency(),
            "memory_percent": self._check_memory_usage(),
            "cpu_percent": self._check_cpu_usage(),
            "stream_length": await self._check_stream_length(),
            "is_healthy": True,
            "recommendation": "normal",
        }

        # Determine if system is healthy
        if health["redis_latency_ms"] > self._redis_latency_threshold_ms:
            health["is_healthy"] = False
            health["recommendation"] = "reduce_batch"

        if health["memory_percent"] > self._memory_threshold_percent:
            health["is_healthy"] = False
            health["recommendation"] = "reject_new"

        if health["cpu_percent"] > self._cpu_threshold_percent:
            health["is_healthy"] = False
            health["recommendation"] = "reduce_batch"

        # Add to history
        self._health_history.append(health)
        if len(self._health_history) > self._max_history:
            self._health_history.pop(0)

        return health

    async def _check_redis_latency(self) -> float:
        """Check Redis latency via PING."""
        try:
            start = datetime.now(timezone.utc)
            await self._redis.ping()  # type: ignore[attr-defined]
            latency_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            return latency_ms
        except Exception:
            log.exception("redis.latency_check_failed")
            return float("inf")

    def _check_memory_usage(self) -> float:
        """Check system memory usage."""
        try:
            return psutil.virtual_memory().percent
        except Exception:
            log.exception("memory.check_failed")
            return 0.0

    def _check_cpu_usage(self) -> float:
        """Check system CPU usage."""
        try:
            return psutil.cpu_percent(interval=0.1)
        except Exception:
            log.exception("cpu.check_failed")
            return 0.0

    async def _check_stream_length(self) -> int:
        """Check Redis Stream length."""
        try:
            return await self._redis.xlen(self._settings.stream_inbound)  # type: ignore[attr-defined]
        except Exception:
            log.exception("stream.length_check_failed")
            return 0

    def adapt_batch_size(self, health: dict[str, Any]) -> int:
        """Adapt batch size based on health status.

        Returns:
            Recommended batch size
        """
        if not health["is_healthy"]:
            # Reduce batch size when unhealthy
            if health["recommendation"] == "reject_new":
                self._current_batch_size = self._min_batch_size
            elif health["recommendation"] == "reduce_batch":
                self._current_batch_size = max(
                    self._min_batch_size,
                    int(self._current_batch_size * 0.5),
                )
        else:
            # Gradually increase batch size when healthy
            if self._current_batch_size < self._base_batch_size:
                self._current_batch_size = min(
                    self._base_batch_size,
                    int(self._current_batch_size * 1.2),
                )
            else:
                self._current_batch_size = self._base_batch_size

        log.debug(
            "adaptive_backpressure.batch_size_adjusted",
            old_batch_size=self._base_batch_size,
            new_batch_size=self._current_batch_size,
            health=health,
        )

        return self._current_batch_size

    async def should_accept_new_event(self) -> tuple[bool, str]:
        """Determine if system should accept new events.

        Returns:
            (should_accept, reason)
        """
        health = await self.check_health()

        if not health["is_healthy"]:
            if health["recommendation"] == "reject_new":
                return False, f"System degraded: {health}"
            elif health["recommendation"] == "reduce_batch":
                # Accept but with reduced batch size
                return True, f"Reduced batch size to {self.adapt_batch_size(health)}"

        return True, "System healthy"

    def get_health_history(self) -> list[dict]:
        """Get health history for monitoring."""
        return self._health_history

    def get_current_batch_size(self) -> int:
        """Get current adapted batch size."""
        return self._current_batch_size


class BackpressureMiddleware:
    """Middleware for applying backpressure at the API level."""

    def __init__(self, backpressure: AdaptiveBackpressure) -> None:
        self._backpressure = backpressure

    async def check_before_ingest(self) -> tuple[bool, int, str]:
        """Check if ingest should proceed.

        Returns:
            (should_proceed, status_code, reason)
        """
        should_accept, reason = await self._backpressure.should_accept_new_event()

        if not should_accept:
            return False, 503, reason

        return True, 200, reason
