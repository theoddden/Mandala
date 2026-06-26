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
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

from mandala.core.state import StateStore

_LATCH_CACHE_MAX_SIZE = 10_000


class _BoundedLRU:
    """Bounded LRU cache for last-committed event times per entity."""

    def __init__(self, maxsize: int = _LATCH_CACHE_MAX_SIZE) -> None:
        self._data: OrderedDict[str, datetime] = OrderedDict()
        self._maxsize = maxsize

    def get(self, key: str) -> datetime | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key: str, value: datetime) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        if len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def pop(self, key: str, default: datetime | None = None) -> datetime | None:
        """Remove and return value for key, or default if not found."""
        return self._data.pop(key, default)

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._data.clear()


log = structlog.get_logger(__name__)

# Rust acceleration for Stator's Latch decision logic
try:
    from mandala_rust_ext import stator_latch_check as rust_stator_latch_check

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False


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
        self._local_cache: _BoundedLRU = _BoundedLRU()
        self._cache_lock = asyncio.Lock()

        # Statistics
        self._stats = defaultdict(int)

        # Reverse-tracking latency: time from dead zone end to catch-up
        self._dead_zone_start: dict[str, datetime] = {}
        self._catch_up_complete: dict[str, datetime] = {}

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
            cached = self._local_cache.get(source_id)
            if cached is not None:
                return cached

        # Check Redis
        raw = await self._redis.get(self._latch_key(source_id))  # type: ignore[attr-defined]
        if not raw:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode()

        try:
            committed_time = datetime.fromisoformat(raw)
            # Update local cache (bounded LRU)
            async with self._cache_lock:
                self._local_cache.set(source_id, committed_time)
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
        # Update local cache (bounded LRU)
        async with self._cache_lock:
            self._local_cache.set(source_id, event_time)

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
        last_committed_str = last_committed.isoformat() if last_committed else None
        event_time_str = event_time.isoformat()

        # Use Rust for decision logic if available (non-blocking, preserves async architecture)
        if _RUST_EXT_AVAILABLE:
            rust_result = rust_stator_latch_check(event_time_str, tolerance_seconds, last_committed_str)
            decision_str = rust_result.decision
            reason = rust_result.reason
            time_diff = rust_result.time_diff_seconds
            lag_seconds = rust_result.lag_seconds
        else:
            # Fallback to Python logic
            if last_committed is None:
                decision_str = "proceed"
                reason = "first_event"
                time_diff = None
                lag_seconds = None
            else:
                time_diff = abs((event_time - last_committed).total_seconds())
                if time_diff <= tolerance_seconds:
                    decision_str = "duplicate"
                    reason = "duplicate_within_tolerance"
                    lag_seconds = None
                elif event_time < last_committed:
                    decision_str = "backfill"
                    reason = "event_time_before_last_committed"
                    lag_seconds = (last_committed - event_time).total_seconds()
                else:
                    decision_str = "proceed"
                    reason = "event_time_after_last_committed"
                    lag_seconds = None

        # Convert decision string to enum
        decision = LatchDecision(decision_str)

        # No prior events - this is the first event for this entity
        if last_committed is None:
            await self.commit_time(source_id, event_time)
            self._stats["first_events"] += 1
            return LatchResult(
                decision=decision,
                last_committed_time=None,
                reason=reason,
                metadata={"first_event": True},
            )

        # Handle duplicate
        if decision == LatchDecision.DUPLICATE:
            self._stats["duplicates"] += 1
            return LatchResult(
                decision=decision,
                last_committed_time=last_committed,
                reason=reason,
                metadata={
                    "time_diff_seconds": time_diff,
                    "tolerance_seconds": tolerance_seconds,
                },
            )

        # Handle time-travel (event is older than last committed)
        if decision == LatchDecision.BACKFILL:
            self._stats["time_travel"] += 1
            self._stats["time_travel_lag_total"] += lag_seconds or 0.0

            # Mark dead zone start for reverse-tracking latency
            if source_id not in self._dead_zone_start:
                self._dead_zone_start[source_id] = event_time

            log.info(
                "stator_latch.time_travel_detected",
                source_id=source_id,
                event_time=event_time,
                last_committed=last_committed,
                lag_seconds=lag_seconds,
                geometric_hash=geometric_hash,
            )

            return LatchResult(
                decision=decision,
                last_committed_time=last_committed,
                reason=reason,
                metadata={
                    "lag_seconds": lag_seconds,
                    "geometric_hash": geometric_hash,
                },
            )

        # Event is in-order - advance the latch
        await self.commit_time(source_id, event_time)
        self._stats["proceed"] += 1
        self._stats["time_delta_total"] += time_diff or 0.0

        # Check if we're catching up from a dead zone
        if source_id in self._dead_zone_start:
            self._catch_up_complete[source_id] = datetime.now(UTC)
            catch_up_latency = (self._catch_up_complete[source_id] - self._dead_zone_start[source_id]).total_seconds()
            self._stats["reverse_tracking_latency_total"] += catch_up_latency
            self._stats["reverse_tracking_events"] += 1

            log.info(
                "stator_latch.catch_up_complete",
                source_id=source_id,
                dead_zone_start=self._dead_zone_start[source_id].isoformat(),
                catch_up_complete=self._catch_up_complete[source_id].isoformat(),
                catch_up_latency_seconds=catch_up_latency,
            )

            # Clean up after catch-up
            del self._dead_zone_start[source_id]
            del self._catch_up_complete[source_id]

        return LatchResult(
            decision=decision,
            last_committed_time=last_committed,
            reason=reason,
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

    async def get_stats(self) -> dict[str, int | float]:
        """Get latch statistics including reverse-tracking latency."""
        stats = dict(self._stats)
        # Calculate average reverse-tracking latency
        if self._stats.get("reverse_tracking_events", 0) > 0:
            stats["reverse_tracking_latency_avg_seconds"] = (
                self._stats.get("reverse_tracking_latency_total", 0) / self._stats["reverse_tracking_events"]
            )
        return stats

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
