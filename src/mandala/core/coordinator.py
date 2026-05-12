"""Connector coordinator - manages connector lifecycle.

Simple registry pattern with asyncio.gather for parallel startup
and graceful shutdown via task cancellation.
"""
from __future__ import annotations

import asyncio

import structlog

from mandala.core.connector import Connector

log = structlog.get_logger(__name__)


class ConnectorCoordinator:
    """Manages connector lifecycle - start, stop, health.
    
    No fancy framework - just asyncio.gather and graceful shutdown.
    """
    
    def __init__(self) -> None:
        self._connectors: dict[str, Connector] = {}
        self._tasks: dict[str, asyncio.Task] = {}
    
    def register(self, connector: Connector) -> None:
        """Register a connector with the coordinator."""
        self._connectors[connector.name] = connector
        log.info("coordinator.register", connector=connector.name)
    
    async def start_all(self) -> None:
        """Start all connectors in parallel."""
        log.info("coordinator.starting", count=len(self._connectors))
        
        for name, connector in self._connectors.items():
            self._tasks[name] = asyncio.create_task(connector.run())
        
        # Wait for all (or any to fail)
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
    
    async def stop_all(self) -> None:
        """Graceful shutdown - cancel all tasks."""
        log.info("coordinator.stopping")
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
    
    def health(self) -> dict[str, bool]:
        """Return health status of all connectors."""
        return {name: task.done() for name, task in self._tasks.items()}
