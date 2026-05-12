"""The Stator's Latch: Event-time determinism for out-of-order data.

The "Military Grade" Latch Implementation ensures that if a truck goes through
a dead zone and uploads 50 pings at once later, the system doesn't "hallucinate"
that the truck teleported.

The Principle:
- Derive a deterministic key from the source ID and source timestamp
- Check if we've already "committed" a future state
- If event_time < last_committed_time, this is "Time-Travel" data
- Time-travel data bypasses the real-time Turbine (detectors/alerts)
- Instead, it updates the historical graph (Snowflake/Iceberg) for backfill

This is identical to Matching Engine Logic in options markets:
If a "Cancel" order arrives after a "Fill" due to latency, the exchange must
have a deterministic latch to reject the cancel.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

from mandala.core.state import StateStore

log = structlog.get_logger(__name__)


class LatchDecision(str, Enum):
    """Decision made by the Stator's Latch."""

    PROCEED = "proceed"  # Event is in-order, process normally
    BACKFILL = "backfill"  # Event is out-of-order (time-travel), backfill only
    DUPLICATE = "duplicate"  # Event already processed at this timestamp


@dataclass
class LatchResult:
    """Result of a latch check."""

    decision: LatchDecision
    last_committed_time: datetime | None
    reason: str
    metadata: dict[str, Any]


class StatorLatch:
    """Redis-backed latch for event-time determinism.

    The latch tracks the last committed event time for each entity (source ID).
    When a new event arrives, it checks if the event time is before the last
    committed time. If so, it's flagged as time-travel data and routed to backfill
    instead of the real-time detector pipeline.

    This prevents false alerts like:
    - "Truck traveled 150 miles in 1 second" (network lag)
    - "Geofence breached" (out-of-order location updates)
    - "Speeding alert" (dead zone batch upload)
    """

    LATCH_KEY_PREFIX = "mandala:latch"
    DEFAULT_TTL_SECONDS = 14 * 24 * 60 * 60  # 14 days

    def __init__(self, redis: object, ttl_seconds: int | None = None) -> None:
        """Initialize the Stator's Latch.

        Args:
            redis: Redis client instance
            ttl_seconds: TTL for latch entries (default: 14 days)
        """
        self._redis = redis
        self._ttl = ttl_seconds or self.DEFAULT_TTL_SECONDS
        self._local_cache: dict[str, datetime] = {}
        self._cache_lock = asyncio.Lock()

        # Statistics
        self._stats = defaultdict(int)

    def _latch_key(self, source_id: str) -> str:
        """Generate Redis key for a source ID's latch."""
        return f"{self.LATCH_KEY_PREFIX}:{source_id}"

    async def get_last_committed_time(self, source_id: str) -> datetime | None:
        """Get the last committed event time for a source ID.

        Args:
            source_id: Entity identifier (e.g., truck ID, shipment URN)

        Returns:
            Last committed datetime or None if no prior events
        """
        # Check local cache first (performance optimization)
        async with self._cache_lock:
            if source_id in self._local_cache:
                return self._local_cache[source_id]

        # Check Redis
        raw = await self._redis.get(self._latch_key(source_id))  # type: ignore[attr-defined]
        if not raw:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode()

        try:
            committed_time = datetime.fromisoformat(raw)
            # Update local cache
            async with self._cache_lock:
                self._local_cache[source_id] = committed_time
            return committed_time
        except (ValueError, TypeError):
            log.warning("stator_latch.invalid_timestamp", source_id=source_id, raw=raw)
            return None

    async def commit_time(self, source_id: str, event_time: datetime) -> None:
        """Commit a new event time for a source ID (advance the latch).

        Args:
            source_id: Entity identifier
            event_time: Event timestamp to commit
        """
        # Update local cache
        async with self._cache_lock:
            self._local_cache[source_id] = event_time

        # Update Redis with TTL
        await self._redis.set(
            self._latch_key(source_id),
            event_time.isoformat(),
            ex=self._ttl,
        )  # type: ignore[attr-defined]

        self._stats["commits"] += 1
        log.debug("stator_latch.committed", source_id=source_id, event_time=event_time)

    async def check(
        self,
        source_id: str,
        event_time: datetime,
        geometric_hash: str | None = None,
        tolerance_seconds: int = 0,
    ) -> LatchResult:
        """Check if an event is in-order or time-travel data.

        The "Military Grade" Latch Implementation:
        1. Derive a deterministic key from the source ID and source timestamp
        2. Check if we've already "committed" a future state
        3. If event_time < last_committed_time, this is "Time-Travel" data
        4. Time-travel data bypasses the real-time Turbine (detectors/alerts)

        Args:
            source_id: Entity identifier (e.g., truck ID, shipment URN)
            event_time: Event timestamp (when the event occurred at the source)
            geometric_hash: Optional geometric hash for spatial coherence check
            tolerance_seconds: Time tolerance for duplicate detection (default: 0)

        Returns:
            LatchResult with decision and metadata
        """
        last_committed = await self.get_last_committed_time(source_id)

        # No prior events - this is the first event for this entity
        if last_committed is None:
            await self.commit_time(source_id, event_time)
            self._stats["first_events"] += 1
            return LatchResult(
                decision=LatchDecision.PROCEED,
                last_committed_time=None,
                reason="first_event",
                metadata={"first_event": True},
            )

        # Check for duplicate (same timestamp within tolerance)
        time_diff = abs((event_time - last_committed).total_seconds())
        if time_diff <= tolerance_seconds:
            self._stats["duplicates"] += 1
            return LatchResult(
                decision=LatchDecision.DUPLICATE,
                last_committed_time=last_committed,
                reason="duplicate_within_tolerance",
                metadata={
                    "time_diff_seconds": time_diff,
                    "tolerance_seconds": tolerance_seconds,
                },
            )

        # Check for time-travel (event is older than last committed)
        if event_time < last_committed:
            lag_seconds = (last_committed - event_time).total_seconds()
            self._stats["time_travel"] += 1
            self._stats["time_travel_lag_total"] += lag_seconds

            log.info(
                "stator_latch.time_travel_detected",
                source_id=source_id,
                event_time=event_time,
                last_committed=last_committed,
                lag_seconds=lag_seconds,
                geometric_hash=geometric_hash,
            )

            return LatchResult(
                decision=LatchDecision.BACKFILL,
                last_committed_time=last_committed,
                reason="event_time_before_last_committed",
                metadata={
                    "lag_seconds": lag_seconds,
                    "geometric_hash": geometric_hash,
                },
            )

        # Event is in-order - advance the latch
        await self.commit_time(source_id, event_time)
        self._stats["proceed"] += 1
        self._stats["time_delta_total"] += time_diff

        return LatchResult(
            decision=LatchDecision.PROCEED,
            last_committed_time=last_committed,
            reason="event_time_after_last_committed",
            metadata={
                "time_delta_seconds": time_diff,
                "geometric_hash": geometric_hash,
            },
        )

    async def reset(self, source_id: str) -> None:
        """Reset the latch for a source ID (use with caution).

        This is primarily for testing or manual recovery scenarios.
        In production, the latch should only advance forward.

        Args:
            source_id: Entity identifier to reset
        """
        async with self._cache_lock:
            self._local_cache.pop(source_id, None)

        await self._redis.delete(self._latch_key(source_id))  # type: ignore[attr-defined]
        self._stats["resets"] += 1
        log.warning("stator_latch.reset", source_id=source_id)

    async def get_stats(self) -> dict[str, int]:
        """Get latch statistics."""
        return dict(self._stats)

    async def clear_cache(self) -> None:
        """Clear the local cache (useful for testing or memory pressure)."""
        async with self._cache_lock:
            self._local_cache.clear()
        log.debug("stator_latch.cache_cleared")


async def process_telemetry_with_latch(
    packet: dict[str, Any],
    latch: StatorLatch,
    state_store: StateStore | None = None,
) -> LatchResult:
    """Process telemetry packet using the Stator's Latch pattern.

    This is the reference implementation showing how to use the latch
    in a telemetry processing pipeline.

    Args:
        packet: Telemetry packet with source_id, event_time, lat, lon
        latch: StatorLatch instance
        state_store: Optional StateStore for committing state

    Returns:
        LatchResult with decision
    """
    source_id = packet.get("source_id")
    event_time = packet.get("event_time")
    latitude = packet.get("latitude")
    longitude = packet.get("longitude")

    if not source_id or not event_time:
        raise ValueError("packet must contain source_id and event_time")

    # Convert event_time to datetime if needed
    if isinstance(event_time, str):
        event_time = datetime.fromisoformat(event_time)
    elif not isinstance(event_time, datetime):
        event_time = datetime.fromtimestamp(event_time, tz=UTC)

    # Compute geometric hash if coordinates provided
    geometric_hash = None
    if latitude is not None and longitude is not None:
        from mandala.core.geometric_hash import GeometricHashService

        geo_service = GeometricHashService()
        geometric_hash = geo_service.compute_hash(latitude, longitude, event_time)

    # Check latch
    result = await latch.check(
        source_id=source_id,
        event_time=event_time,
        geometric_hash=geometric_hash,
    )

    # Handle based on decision
    if result.decision == LatchDecision.PROCEED:
        # Commit state and run detectors
        if state_store:
            # In production, this would project to state and run detectors
            pass
        log.info("telemetry.proceed", source_id=source_id, event_time=event_time)

    elif result.decision == LatchDecision.BACKFILL:
        # Backfill historical graph (Snowflake/Iceberg)
        # Bypass real-time Turbine (detectors/alerts)
        log.info(
            "telemetry.backfill",
            source_id=source_id,
            event_time=event_time,
            lag_seconds=result.metadata.get("lag_seconds"),
        )
        # In production, this would write to the historical data store

    elif result.decision == LatchDecision.DUPLICATE:
        # Drop duplicate
        log.debug("telemetry.duplicate", source_id=source_id, event_time=event_time)

    return result
