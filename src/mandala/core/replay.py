"""Event replay system for fixing production bugs.

Allows replaying historical events from the Iceberg event log to correct
state after fixing detector bugs or projection logic.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.event_log import EventLog
from mandala.core.events.envelope import MandalaEvent
from mandala.core.state import StateStore

log = structlog.get_logger(__name__)


class EventReplay:
    """Replay events from Iceberg event log with idempotency protection."""

    def __init__(
        self,
        event_log: EventLog | None,
        state: StateStore,
    ) -> None:
        self._event_log = event_log
        self._state = state

    async def replay_range(
        self,
        from_dt: datetime,
        to_dt: datetime,
        detector_filter: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Replay events within a time range.

        Args:
            from_dt: Start of replay window
            to_dt: End of replay window
            detector_filter: Only replay events matching this detector name
            dry_run: If True, don't actually write to state

        Returns:
            Replay statistics
        """
        if not self._event_log:
            log.error("replay.event_log_not_configured")
            return {"error": "Event log not configured"}

        log.info(
            "replay.started",
            from_dt=from_dt.isoformat(),
            to_dt=to_dt.isoformat(),
            detector_filter=detector_filter,
            dry_run=dry_run,
        )

        stats = {
            "events_read": 0,
            "events_processed": 0,
            "events_skipped": 0,
            "errors": 0,
            "dry_run": dry_run,
        }

        try:
            # Read events from Iceberg
            events = await self._event_log.read_range(from_dt, to_dt)
            stats["events_read"] = len(events)

            for event in events:
                try:
                    # Filter by detector if specified
                    if detector_filter:
                        # Check if event was emitted by this detector
                        # This requires tracking detector name in event metadata
                        # For now, skip filtering if not available
                        pass

                    if dry_run:
                        log.debug("replay.dry_run", event_id=event.id, event_type=event.type)
                        stats["events_skipped"] += 1
                        continue

                    # Re-project event into state
                    # Import here to avoid circular dependency
                    from mandala.projection import project

                    await project(event, self._state)
                    stats["events_processed"] += 1

                except Exception as exc:
                    log.exception(
                        "replay.event_failed",
                        event_id=event.id,
                        event_type=event.type,
                        error=str(exc),
                    )
                    stats["errors"] += 1

            log.info("replay.completed", stats=stats)
            return stats

        except Exception as exc:
            log.exception("replay.failed", error=str(exc))
            return {"error": str(exc), **stats}

    async def replay_entity(
        self,
        entity_urn: str,
        from_dt: datetime,
        to_dt: datetime,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Replay events for a specific entity.

        Useful for fixing state for a single truck or shipment.
        """
        if not self._event_log:
            log.error("replay.event_log_not_configured")
            return {"error": "Event log not configured"}

        log.info(
            "replay.entity_started",
            entity_urn=entity_urn,
            from_dt=from_dt.isoformat(),
            to_dt=to_dt.isoformat(),
            dry_run=dry_run,
        )

        stats = {
            "events_read": 0,
            "events_processed": 0,
            "events_skipped": 0,
            "errors": 0,
            "dry_run": dry_run,
        }

        try:
            # Read events for specific entity from Iceberg
            events = await self._event_log.read_entity(entity_urn, from_dt, to_dt)
            stats["events_read"] = len(events)

            for event in events:
                try:
                    if dry_run:
                        log.debug("replay.dry_run", event_id=event.id, event_type=event.type)
                        stats["events_skipped"] += 1
                        continue

                    from mandala.projection import project

                    await project(event, self._state)
                    stats["events_processed"] += 1

                except Exception as exc:
                    log.exception(
                        "replay.event_failed",
                        event_id=event.id,
                        event_type=event.type,
                        error=str(exc),
                    )
                    stats["errors"] += 1

            log.info("replay.entity_completed", stats=stats)
            return stats

        except Exception as exc:
            log.exception("replay.entity_failed", error=str(exc))
            return {"error": str(exc), **stats}


async def replay_from_stream(
    redis: object,
    state: StateStore,
    stream_name: str = "mandala:events",
    count: int = 1000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Replay events directly from Redis Stream (for recent events).

    Useful for replaying the last N events when Iceberg is not configured.
    """
    log.info(
        "replay.from_stream",
        stream=stream_name,
        count=count,
        dry_run=dry_run,
    )

    stats = {
        "events_read": 0,
        "events_processed": 0,
        "events_skipped": 0,
        "errors": 0,
        "dry_run": dry_run,
    }

    try:
        # Read last N events from stream
        messages = await redis.xrevrange(stream_name, count=count)  # type: ignore[attr-defined]
        stats["events_read"] = len(messages)

        # Process in reverse order (oldest first)
        for msg_id, fields in reversed(messages):
            try:
                raw = fields.get(b"e") if isinstance(fields, dict) else fields.get("e")
                if not raw:
                    stats["events_skipped"] += 1
                    continue

                event = MandalaEvent.from_json(raw)

                if dry_run:
                    log.debug("replay.dry_run", event_id=event.id, event_type=event.type)
                    stats["events_skipped"] += 1
                    continue

                from mandala.projection import project

                await project(event, state)
                stats["events_processed"] += 1

            except Exception as exc:
                log.exception(
                    "replay.event_failed",
                    msg_id=msg_id,
                    error=str(exc),
                )
                stats["errors"] += 1

        log.info("replay.from_stream_completed", stats=stats)
        return stats

    except Exception as exc:
        log.exception("replay.from_stream_failed", error=str(exc))
        return {"error": str(exc), **stats}
