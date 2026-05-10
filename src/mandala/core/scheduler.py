"""Simple async scheduler for periodic tasks.

No external dependencies - just asyncio and datetime.
Supports cron-style scheduling: interval, fixed times, etc.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

import structlog

log = structlog.get_logger(__name__)


class Scheduler:
    """Simple async scheduler for periodic connector polling.
    
    Supports:
    - Fixed interval (every N seconds/minutes/hours)
    - Cron-style (daily at specific time)
    - Custom schedules via callable
    """
    
    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._running = False
    
    def schedule_interval(
        self,
        name: str,
        func: Callable[[], Any],
        interval_seconds: float,
    ) -> None:
        """Schedule a function to run at a fixed interval."""
        async def _loop() -> None:
            while self._running:
                try:
                    await func()
                except Exception as exc:
                    log.exception("scheduler.error", task=name, error=str(exc))
                await asyncio.sleep(interval_seconds)
        
        self._tasks[name] = asyncio.create_task(_loop())
        log.info("scheduler.scheduled", name=name, interval=interval_seconds)
    
    def schedule_daily(
        self,
        name: str,
        func: Callable[[], Any],
        hour: int,
        minute: int = 0,
        timezone_str: str = "UTC",
    ) -> None:
        """Schedule a function to run daily at a specific time."""
        async def _loop() -> None:
            while self._running:
                now = datetime.now(timezone.utc)
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                # If target time has passed today, schedule for tomorrow
                if now >= target:
                    target += timedelta(days=1)
                
                sleep_seconds = (target - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
                
                try:
                    await func()
                except Exception as exc:
                    log.exception("scheduler.error", task=name, error=str(exc))
        
        self._tasks[name] = asyncio.create_task(_loop())
        log.info("scheduler.scheduled", name=name, time=f"{hour:02d}:{minute:02d}")
    
    async def start(self) -> None:
        """Start all scheduled tasks."""
        self._running = True
        log.info("scheduler.starting", count=len(self._tasks))
    
    async def stop(self) -> None:
        """Stop all scheduled tasks."""
        self._running = False
        log.info("scheduler.stopping")
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
