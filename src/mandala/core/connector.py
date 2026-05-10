"""Base class for Mandala connectors.

All connectors (webhook, poll, file, CDC) inherit from this to get:
- Async lifecycle management (__aenter__/__aexit__)
- Health reporting via Prometheus
- Structured logging
- Graceful shutdown
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog
from prometheus_client import Counter, Gauge

log = structlog.get_logger(__name__)

connector_healthy = Gauge(
    "mandala_connector_healthy",
    "Connector health status (1=healthy, 0=unhealthy)",
    ["connector"]
)

connector_events_published = Counter(
    "mandala_connector_events_published_total",
    "Total events published by connector",
    ["connector"]
)

connector_errors = Counter(
    "mandala_connector_errors_total",
    "Total errors encountered by connector",
    ["connector"]
)


class Connector(ABC):
    """Base class for all Mandala connectors.
    
    Each connector is an async context manager that:
    - Registers with the coordinator on startup
    - Publishes events to the EventBus
    - Reports health via Prometheus
    - Shuts down gracefully
    """
    
    def __init__(self, name: str, bus: "EventBus") -> None:
        self.name = name
        self._bus = bus
        self._running = False
    
    @abstractmethod
    async def _run(self) -> None:
        """Connector-specific run logic (poll, watch, etc.)."""
        ...
    
    async def __aenter__(self) -> "Connector":
        self._running = True
        log.info("connector.start", connector=self.name)
        connector_healthy.labels(connector=self.name).set(1)
        return self
    
    async def __aexit__(self, *args: Any) -> None:
        self._running = False
        log.info("connector.stop", connector=self.name)
        connector_healthy.labels(connector=self.name).set(0)
    
    async def run(self) -> None:
        """Main loop - runs until shutdown."""
        async with self:
            while self._running:
                try:
                    await self._run()
                except Exception as exc:
                    log.exception("connector.error", connector=self.name, error=str(exc))
                    connector_errors.labels(connector=self.name).inc()
                    await asyncio.sleep(5)  # Backoff
