"""Re-ordering Buffer for out-of-order event handling.

The "Clever" Bit: Mandala doesn't just store the location; it stores a Vector
of Delta-T. If an event arrives out of sequence, the "Stator" uses a
Re-ordering Buffer to "re-wind" the state of the asset, insert the missing
data point, and re-calculate the trajectory before the "Turbine" (Palantir)
ever sees it.

This buffer holds events that arrive out-of-order and re-sequences them
before they reach the detector pipeline. It maintains a sliding window of
events per entity and ensures temporal ordering.
"""

from __future__ import annotations

import asyncio
import heapq
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.state import StateStore
from mandala.settings import get_settings

log = structlog.get_logger(__name__)

# Rust acceleration for reorder buffer logic
try:
    from mandala_rust_ext import reorder_buffer_is_ready as rust_reorder_buffer_is_ready
    from mandala_rust_ext import reorder_buffer_should_buffer as rust_reorder_buffer_should_buffer

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False


@dataclass(order=True)
class BufferedEvent:
    """An event held in the re-ordering buffer."""

    event_time: datetime = field(compare=True)
    event: MandalaEvent = field(compare=False)
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC), compare=False)
    retry_count: int = field(default=0, compare=False)


@dataclass
class BufferStats:
    """Statistics for the re-ordering buffer."""

    total_buffered: int = 0
    total_released: int = 0
    total_expired: int = 0
    total_dropped: int = 0
    avg_buffer_time_ms: float = 0.0


class ReorderBuffer:
    """Re-ordering buffer for out-of-order events.

    Maintains a priority queue per entity (source ID) to re-sequence events
    that arrive out of order due to network latency, dead zones, or other
    transmission issues.

    The buffer:
    1. Holds events until they can be released in temporal order
    2. Supports a configurable window size (max events or max time)
    3. Expels stale events that are too old to be relevant
    4. Provides statistics for monitoring buffer health

    When an event arrives:
    - If it's the next expected event (in-order), release immediately
    - If it's from the past (older than expected), buffer it
    - If it's from the future (gap detected), buffer it and wait for missing events

    The buffer periodically releases events that are:
    - Ready (all prior events received)
    - Expired (waited too long, release even with gaps)
    """

    BUFFER_KEY_PREFIX = "mandala:reorder"
    DEFAULT_MAX_EVENTS_PER_ENTITY = 100
    DEFAULT_MAX_WAIT_SECONDS = 300  # 5 minutes
    DEFAULT_EXPIRE_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        redis: object | None = None,
        max_events_per_entity: int | None = None,
        max_wait_seconds: int | None = None,
        expire_seconds: int | None = None,
    ) -> None:
        """Initialize the re-ordering buffer.

        Args:
            redis: Optional Redis client for persistence (can be None for in-memory only)
            max_events_per_entity: Max buffered events per entity (default: 100)
            max_wait_seconds: Max time to wait before releasing with gaps (default: 300s)
            expire_seconds: Max age before dropping events (default: 3600s)
        """
        self._redis = redis
        s = get_settings()
        self._max_events = max_events_per_entity or getattr(
            s, "reorder_buffer_max_events", self.DEFAULT_MAX_EVENTS_PER_ENTITY
        )
        self._max_wait = max_wait_seconds or getattr(
            s, "reorder_buffer_max_wait_seconds", self.DEFAULT_MAX_WAIT_SECONDS
        )
        self._expire_seconds = expire_seconds or getattr(
            s, "reorder_buffer_expire_seconds", self.DEFAULT_EXPIRE_SECONDS
        )

        # In-memory buffers per entity (priority queues — used when no Redis)
        self._buffers: dict[str, list[BufferedEvent]] = defaultdict(list)
        self._next_expected: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

        # Redis key constants (only used when self._redis is not None)
        self._EXPECTED_HASH = "mandala:reorder:expected"

        if self._redis is not None:
            log.info(
                "reorder_buffer.redis_backed",
                message="Buffer backed by Redis — safe for multi-worker deployment",
            )
        else:
            log.warning(
                "reorder_buffer.in_memory",
                message="Buffer is in-memory only. Run multiple workers with MANDALA_REDIS_URL set.",
            )

        # Statistics
        self._stats = BufferStats()
        self._buffer_times: deque[float] = deque(maxlen=1000)

    # ------------------------------------------------------------------
    # Redis persistence helpers
    # ------------------------------------------------------------------

    async def _r_get_next_expected(self, source_id: str) -> datetime | None:
        raw = await self._redis.hget(self._EXPECTED_HASH, source_id)  # type: ignore[union-attr]
        if raw is None:
            return None
        s = raw.decode() if isinstance(raw, bytes) else raw
        return datetime.fromisoformat(s)

    async def _r_set_next_expected(self, source_id: str, dt: datetime) -> None:
        await self._redis.hset(self._EXPECTED_HASH, source_id, dt.isoformat())  # type: ignore[union-attr]

    def _r_buf_key(self, source_id: str) -> str:
        return f"{self.BUFFER_KEY_PREFIX}:{source_id}"

    async def _r_zadd_event(self, source_id: str, event: MandalaEvent, event_time: datetime) -> None:
        key = self._r_buf_key(source_id)
        score = event_time.timestamp()
        member = event.to_json()
        await self._redis.zadd(key, {member: score})  # type: ignore[union-attr]
        await self._redis.expire(key, self._expire_seconds)  # type: ignore[union-attr]

    async def _r_zcard(self, source_id: str) -> int:
        return await self._redis.zcard(self._r_buf_key(source_id))  # type: ignore[union-attr]

    async def _r_zpopmin(self, source_id: str) -> tuple[float, str] | None:
        result = await self._redis.zpopmin(self._r_buf_key(source_id), count=1)  # type: ignore[union-attr]
        if not result:
            return None
        member, score = result[0]
        return score, (member.decode() if isinstance(member, bytes) else member)

    async def _r_zrange_oldest(self, source_id: str) -> tuple[float, str] | None:
        """Peek at the oldest (lowest-score) member without removing it."""
        result = await self._redis.zrange(self._r_buf_key(source_id), 0, 0, withscores=True)  # type: ignore[union-attr]
        if not result:
            return None
        member, score = result[0]
        return score, (member.decode() if isinstance(member, bytes) else member)

    async def _r_get_all_source_ids(self) -> list[str]:
        """Return all source IDs that have a known next_expected value in Redis."""
        raw_keys = await self._redis.hkeys(self._EXPECTED_HASH)  # type: ignore[union-attr]
        return [k.decode() if isinstance(k, bytes) else k for k in raw_keys]

    # ------------------------------------------------------------------

    async def add(
        self,
        event: MandalaEvent,
        source_id: str,
        event_time: datetime | None = None,
    ) -> tuple[bool, MandalaEvent | None]:
        """Add an event to the re-ordering buffer.

        Args:
            event: The event to add
            source_id: Entity identifier (truck URN, etc.)
            event_time: Event timestamp (defaults to event.time)

        Returns:
            Tuple of (should_release_immediately, event_to_release)
        """
        if event_time is None:
            event_time = event.time

        if self._redis is not None:
            return await self._add_redis(event, source_id, event_time)

        async with self._lock:
            # First event for this source - release immediately
            if source_id not in self._next_expected:
                self._next_expected[source_id] = event_time
                self._stats.total_released += 1
                log.debug(
                    "reorder_buffer.first_event",
                    source_id=source_id,
                    event_time=event_time,
                )
                return True, event

            next_expected = self._next_expected[source_id]
            next_expected_str = next_expected.isoformat()
            event_time_str = event_time.isoformat()

            # Use Rust for buffer decision if available (non-blocking, preserves async architecture)
            if _RUST_EXT_AVAILABLE:
                should_buffer, next_expected_update = rust_reorder_buffer_should_buffer(
                    event_time_str,
                    60.0,  # gap_threshold_seconds
                    next_expected_str,
                )
                if not should_buffer:
                    # Release immediately
                    if next_expected_update:
                        self._next_expected[source_id] = datetime.fromisoformat(next_expected_update)
                    else:
                        self._next_expected[source_id] = event_time
                    self._stats.total_released += 1
                    return True, event
                else:
                    # Buffer the event
                    return await self._buffer_event(event, source_id, event_time)

            # Fallback to Python logic
            # Check if event is in-order (>= next expected)
            if event_time >= next_expected:
                # Check if there's a gap (event is significantly newer)
                time_gap = (event_time - next_expected).total_seconds()
                if time_gap > 60:  # More than 1 minute gap
                    log.info(
                        "reorder_buffer.gap_detected",
                        source_id=source_id,
                        next_expected=next_expected,
                        event_time=event_time,
                        gap_seconds=time_gap,
                    )
                    # Buffer this event and wait for missing events
                    # DO NOT update next_expected when buffering due to gap
                    return await self._buffer_event(event, source_id, event_time)

                # Event is in-order and close enough - release immediately
                self._next_expected[source_id] = event_time
                self._stats.total_released += 1

                return True, event

            # Otherwise buffer it
            return await self._buffer_event(event, source_id, event_time)

    async def _add_redis(
        self,
        event: MandalaEvent,
        source_id: str,
        event_time: datetime,
    ) -> tuple[bool, MandalaEvent | None]:
        """Redis-backed add path — safe under multi-worker deployment."""
        next_expected = await self._r_get_next_expected(source_id)
        if next_expected is None:
            # First event for this entity
            await self._r_set_next_expected(source_id, event_time)
            self._stats.total_released += 1
            log.debug("reorder_buffer.first_event", source_id=source_id, event_time=event_time)
            return True, event

        event_time_str = event_time.isoformat()
        next_expected_str = next_expected.isoformat()

        if _RUST_EXT_AVAILABLE:
            should_buffer, next_expected_update = rust_reorder_buffer_should_buffer(
                event_time_str, 60.0, next_expected_str
            )
            if not should_buffer:
                new_next = datetime.fromisoformat(next_expected_update) if next_expected_update else event_time
                await self._r_set_next_expected(source_id, new_next)
                self._stats.total_released += 1
                return True, event
        else:
            if event_time >= next_expected:
                time_gap = (event_time - next_expected).total_seconds()
                if time_gap <= 60:
                    await self._r_set_next_expected(source_id, event_time)
                    self._stats.total_released += 1
                    return True, event

        # Buffer the event in Redis ZSET
        card = await self._r_zcard(source_id)
        if card >= self._max_events:
            dropped = await self._r_zpopmin(source_id)
            self._stats.total_dropped += 1
            log.warning(
                "reorder_buffer.dropped_oldest", source_id=source_id, dropped_score=dropped[0] if dropped else None
            )

        await self._r_zadd_event(source_id, event, event_time)
        self._stats.total_buffered += 1
        log.debug("reorder_buffer.buffered", source_id=source_id, event_time=event_time, buffer_size=card + 1)
        return False, None

    async def _buffer_event(
        self,
        event: MandalaEvent,
        source_id: str,
        event_time: datetime,
    ) -> tuple[bool, MandalaEvent | None]:
        """Buffer an out-of-order event (in-memory path).

        Returns:
            Tuple of (should_release_immediately, event_to_release)
        """
        buffer = self._buffers[source_id]

        # Check buffer size limit
        if len(buffer) >= self._max_events:
            # Drop oldest event
            oldest = heapq.heappop(buffer)
            self._stats.total_dropped += 1
            log.warning(
                "reorder_buffer.dropped_oldest",
                source_id=source_id,
                dropped_event_time=oldest.event_time,
            )

        # Add to buffer
        buffered = BufferedEvent(event_time=event_time, event=event)
        heapq.heappush(buffer, buffered)
        self._stats.total_buffered += 1

        log.debug(
            "reorder_buffer.buffered",
            source_id=source_id,
            event_time=event_time,
            buffer_size=len(buffer),
        )

        return False, None

    async def release_ready(self, source_id: str) -> list[MandalaEvent]:
        """Release all ready events for an entity.

        Ready events are those that are in-order (no gaps) or have waited
        longer than max_wait_seconds.

        Args:
            source_id: Entity identifier

        Returns:
            List of events to release (in temporal order)
        """
        if self._redis is not None:
            return await self._release_ready_redis(source_id)

        async with self._lock:
            buffer = self._buffers[source_id]
            if not buffer:
                return []

            next_expected = self._next_expected.get(source_id)
            if not next_expected:
                return []

            released = []
            now = datetime.now(UTC)
            now_str = now.isoformat()
            next_expected_str = next_expected.isoformat()

            while buffer:
                # Peek at the oldest event
                oldest = buffer[0]
                wait_time = (now - oldest.received_at).total_seconds()
                oldest_event_time_str = oldest.event_time.isoformat()

                # Use Rust for ready check if available (non-blocking, preserves async architecture)
                if _RUST_EXT_AVAILABLE:
                    is_ready = rust_reorder_buffer_is_ready(
                        oldest_event_time_str,
                        next_expected_str,
                        now_str,
                        self._max_wait,
                    )
                    if is_ready:
                        # Release this event
                        heapq.heappop(buffer)
                        released.append(oldest.event)
                        self._next_expected[source_id] = oldest.event_time
                        next_expected_str = oldest.event_time.isoformat()
                        self._stats.total_released += 1

                        # Track buffer time for statistics
                        buffer_time = (now - oldest.received_at).total_seconds() * 1000
                        self._buffer_times.append(buffer_time)

                        log.debug(
                            "reorder_buffer.released",
                            source_id=source_id,
                            event_time=oldest.event_time,
                        )
                    else:
                        # Event not ready yet - stop
                        break
                else:
                    # Fallback to Python logic
                    # Check if event is ready
                    is_in_order = oldest.event_time >= next_expected
                    is_expired = wait_time >= self._max_wait

                    if is_in_order or is_expired:
                        # Release this event
                        heapq.heappop(buffer)
                        released.append(oldest.event)
                        self._next_expected[source_id] = oldest.event_time
                        self._stats.total_released += 1

                        # Track buffer time for statistics
                        buffer_time = (now - oldest.received_at).total_seconds() * 1000
                        self._buffer_times.append(buffer_time)

                        if is_expired and not is_in_order:
                            log.info(
                                "reorder_buffer.released_with_gap",
                                source_id=source_id,
                                event_time=oldest.event_time,
                                next_expected=next_expected,
                                wait_seconds=wait_time,
                            )
                        else:
                            log.debug(
                                "reorder_buffer.released",
                                source_id=source_id,
                                event_time=oldest.event_time,
                            )
                    else:
                        # Event not ready yet - stop
                        break

            # Update average buffer time
            if self._buffer_times:
                self._stats.avg_buffer_time_ms = sum(self._buffer_times) / len(self._buffer_times)

            return released

    async def _release_ready_redis(self, source_id: str) -> list[MandalaEvent]:
        """Redis-backed release_ready."""
        released: list[MandalaEvent] = []
        now = datetime.now(UTC)
        now_str = now.isoformat()
        next_expected = await self._r_get_next_expected(source_id)
        if next_expected is None:
            return []
        next_expected_str = next_expected.isoformat()

        while True:
            entry = await self._r_zrange_oldest(source_id)
            if entry is None:
                break
            score, member_str = entry
            event_time = datetime.fromtimestamp(score, UTC)
            # Estimate received_at as now (Redis doesn't store it; use max_wait)
            wait_time = (now - event_time).total_seconds()
            oldest_str = event_time.isoformat()

            if _RUST_EXT_AVAILABLE:
                is_ready = rust_reorder_buffer_is_ready(oldest_str, next_expected_str, now_str, self._max_wait)
            else:
                is_ready = event_time >= next_expected or wait_time >= self._max_wait

            if not is_ready:
                break

            # Pop and deserialise
            popped = await self._r_zpopmin(source_id)
            if popped is None:
                break
            _, member_str = popped
            try:
                event = MandalaEvent.model_validate_json(member_str)
            except Exception:  # noqa: BLE001
                log.exception("reorder_buffer.redis.deserialize_failed")
                continue

            next_expected = event_time
            next_expected_str = event_time.isoformat()
            await self._r_set_next_expected(source_id, event_time)
            released.append(event)
            self._stats.total_released += 1
            buffer_time_ms = wait_time * 1000
            self._buffer_times.append(buffer_time_ms)

            log.debug("reorder_buffer.released", source_id=source_id, event_time=event_time)

        if self._buffer_times:
            self._stats.avg_buffer_time_ms = sum(self._buffer_times) / len(self._buffer_times)
        return released

    async def release_all(self, source_id: str) -> list[MandalaEvent]:
        """Release ALL buffered events for an entity (emergency flush).

        Use this during shutdown or when an entity is known to be complete.

        Args:
            source_id: Entity identifier

        Returns:
            List of all buffered events (in temporal order)
        """
        async with self._lock:
            buffer = self._buffers[source_id]
            if not buffer:
                return []

            # Sort and release all
            events = []
            while buffer:
                buffered = heapq.heappop(buffer)
                events.append(buffered.event)
                self._stats.total_released += 1
                self._next_expected[source_id] = buffered.event_time

            self._buffers.pop(source_id, None)
            log.info(
                "reorder_buffer.flushed",
                source_id=source_id,
                count=len(events),
            )

            return events

    async def get_stats(self) -> dict[str, Any]:
        """Get buffer statistics."""
        async with self._lock:
            return {
                "total_buffered": self._stats.total_buffered,
                "total_released": self._stats.total_released,
                "total_expired": self._stats.total_expired,
                "total_dropped": self._stats.total_dropped,
                "avg_buffer_time_ms": self._stats.avg_buffer_time_ms,
                "active_entities": len(self._buffers),
                "buffer_sizes": {source_id: len(buffer) for source_id, buffer in self._buffers.items()},
            }

    async def clear(self, source_id: str | None = None) -> None:
        """Clear buffer for an entity or all entities.

        Args:
            source_id: Entity to clear, or None to clear all
        """
        async with self._lock:
            if source_id:
                self._buffers.pop(source_id, None)
                self._next_expected.pop(source_id, None)
                log.debug("reorder_buffer.cleared", source_id=source_id)
            else:
                self._buffers.clear()
                self._next_expected.clear()
                log.debug("reorder_buffer.cleared_all")

    async def rewind_state(
        self,
        source_id: str,
        rewind_to: datetime,
        state_store: StateStore | None = None,
    ) -> dict[str, Any]:
        """Rewind state to a specific point in time.

        When an out-of-order event arrives, we may need to "rewind" the
        state of the asset, insert the missing data point, and re-calculate
        the trajectory.

        **STUB IMPLEMENTATION:** This is a simplified placeholder. Full implementation requires:
        - Event sourcing with full event log (Iceberg or Redis Stream)
        - State snapshots at intervals for performance
        - Incremental re-computation of derived state
        - Detector re-execution from the rewind point

        Current behavior: Resets the next expected time and clears the buffer.
        This allows new events to be processed from the rewind point, but does
        not actually revert previously committed state.

        Args:
            source_id: Entity identifier
            rewind_to: Point in time to rewind to
            state_store: Optional StateStore to rewind (currently unused)

        Returns:
            Metadata about the rewind operation
        """
        log.warning(
            "reorder_buffer.rewind_state_stub",
            source_id=source_id,
            rewind_to=rewind_to.isoformat(),
            message="State rewinding is not fully implemented - only buffer is cleared",
        )

        # For now, we just reset the next expected time and clear buffer
        async with self._lock:
            self._next_expected[source_id] = rewind_to
            # Clear buffer to force re-processing
            self._buffers.pop(source_id, None)

        return {
            "source_id": source_id,
            "rewound_to": rewind_to.isoformat(),
            "buffer_cleared": True,
            "state_reverted": False,  # STUB: actual state reversion not implemented
            "note": "Full state rewinding requires event sourcing implementation",
        }


class ReorderBufferManager:
    """Manager for multiple re-ordering buffers.

    Provides a higher-level interface for managing buffers across
    all entities in the system.
    """

    def __init__(
        self,
        redis: object | None = None,
        on_release: object | None = None,
        **buffer_kwargs: Any,
    ) -> None:
        """Initialize the buffer manager.

        Args:
            redis: Optional Redis client
            on_release: Optional async callable(MandalaEvent) invoked for each
                event released by the background loop. If None, released events
                are only logged (backward-compatible behaviour).
            **buffer_kwargs: Arguments passed to ReorderBuffer constructor
        """
        self._buffer = ReorderBuffer(redis=redis, **buffer_kwargs)
        self._on_release = on_release
        self._background_task: asyncio.Task | None = None
        self._running = False

    async def start(self, check_interval_seconds: float = 5.0) -> None:
        """Start the background task that periodically releases ready events.

        Args:
            check_interval_seconds: How often to check for ready events
        """
        if self._running:
            return

        self._running = True
        self._background_task = asyncio.create_task(self._release_loop(check_interval_seconds))
        log.info("reorder_buffer.started", check_interval=check_interval_seconds)

    async def stop(self) -> None:
        """Stop the background task and flush all buffers."""
        self._running = False
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass

        log.info("reorder_buffer.stopped")

    async def _release_loop(self, interval_seconds: float) -> None:
        """Background loop that periodically releases ready events."""
        while self._running:
            try:
                # Get all entities with buffered events
                if self._buffer._redis is not None:
                    entities = await self._buffer._r_get_all_source_ids()
                else:
                    async with self._buffer._lock:
                        entities = list(self._buffer._buffers.keys())

                # Release ready events for each entity
                for source_id in entities:
                    released = await self._buffer.release_ready(source_id)
                    if released:
                        log.debug(
                            "reorder_buffer.batch_released",
                            source_id=source_id,
                            count=len(released),
                        )
                        if self._on_release:
                            for event in released:
                                try:
                                    await self._on_release(event)
                                except Exception:  # noqa: BLE001
                                    log.exception(
                                        "reorder_buffer.release_callback_failed",
                                        event_id=event.id,
                                    )

                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                log.exception("reorder_buffer.release_loop_error")
                await asyncio.sleep(interval_seconds)

    async def add_event(
        self,
        event: MandalaEvent,
        source_id: str,
        event_time: datetime,
    ) -> tuple[bool, MandalaEvent | None]:
        """Add an event to the buffer.

        Wrapper around ReorderBuffer.add for convenience.

        Returns:
            Tuple of (should_release_immediately, event_to_release)
        """
        return await self._buffer.add(event, source_id, event_time)

    async def get_stats(self) -> dict[str, Any]:
        """Get buffer statistics."""
        return await self._buffer.get_stats()
