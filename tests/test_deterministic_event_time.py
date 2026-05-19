"""Tests for Deterministic Event-Time Windowing with Geometric Hashing.

Tests the Stator's Latch, Geometric Hashing, and Re-ordering Buffer
functionality that prevents state-machine corruption from out-of-order
spatial data.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from mandala.core.events.envelope import MandalaEvent
from mandala.core.geometric_hash import (
    GeometricHashProvider,
    GeometricHashService,
    generate_geometric_idempotency_key,
)
from mandala.core.reorder_buffer import ReorderBuffer, ReorderBufferManager
from mandala.core.stator_latch import LatchDecision, StatorLatch, process_telemetry_with_latch


class TestGeometricHashService:
    """Test geometric hashing service."""

    def test_geohash_fallback(self):
        """Test fallback geohash encoding when H3/S2 not available."""
        service = GeometricHashService(provider=GeometricHashProvider.NONE)
        lat, lon = 29.4267, -98.4893  # San Antonio, TX
        event_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

        hash_val = service.compute_hash(lat, lon, event_time)

        assert hash_val is not None
        assert isinstance(hash_val, str)
        assert len(hash_val) == 16  # SHA256[:16]

    def test_compute_delta_t_vector(self):
        """Test Delta-T vector computation for trajectory analysis."""
        service = GeometricHashService(provider=GeometricHashProvider.NONE)

        current_hash = service.compute_hash(29.4267, -98.4893, datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC))
        previous_hash = service.compute_hash(27.5060, -99.4969, datetime(2026, 5, 11, 11, 0, 0, tzinfo=UTC))

        current_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        previous_time = datetime(2026, 5, 11, 11, 0, 0, tzinfo=UTC)

        vector = service.compute_delta_t_vector(current_hash, previous_hash, current_time, previous_time)

        assert vector["delta_t_seconds"] == 3600.0
        assert vector["hash_changed"] is True
        assert vector["spatial_delta"] is not None
        assert vector["velocity_mps"] is not None

    def test_spatial_coherence_check(self):
        """Test spatial coherence check for detecting impossible movement."""
        service = GeometricHashService(provider=GeometricHashProvider.NONE)

        # Normal movement (1 hour, 50 miles) - should be coherent
        assert (
            service.is_spatially_coherent(
                delta_t_seconds=3600,
                velocity_mps=22.35,  # ~50 mph
                max_velocity_mps=150.0,
            )
            is True
        )

        # Impossible movement (1 second, 150 miles) - should be incoherent
        assert (
            service.is_spatially_coherent(
                delta_t_seconds=1.0,
                velocity_mps=67056.0,  # ~150 miles in 1 second
                max_velocity_mps=150.0,
            )
            is False
        )

        # Time travel (negative delta) - should be incoherent
        assert (
            service.is_spatially_coherent(
                delta_t_seconds=-1.0,
                velocity_mps=0.0,
                max_velocity_mps=150.0,
            )
            is False
        )

    def test_generate_geometric_idempotency_key(self):
        """Test geometric idempotency key generation."""
        source_id = "truck-123"
        event_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        lat, lon = 29.4267, -98.4893

        key1 = generate_geometric_idempotency_key(source_id, event_time, lat, lon)
        key2 = generate_geometric_idempotency_key(source_id, event_time, lat, lon)

        # Same inputs should produce same key
        assert key1 == key2

        # Different coordinates should produce different key
        key3 = generate_geometric_idempotency_key(source_id, event_time, 27.5060, -99.4969)
        assert key1 != key3


class TestStatorLatch:
    """Test Stator's Latch for event-time determinism."""

    @pytest.fixture
    def redis_mock(self):
        """Mock Redis client for testing."""

        class MockRedis:
            def __init__(self):
                self.data = {}

            async def get(self, key):
                return self.data.get(key)

            async def set(self, key, value, ex=None):
                self.data[key] = value

            async def delete(self, key):
                self.data.pop(key, None)

        return MockRedis()

    @pytest.mark.asyncio
    async def test_first_event_proceeds(self, redis_mock):
        """Test that the first event for an entity proceeds."""
        latch = StatorLatch(redis_mock)
        source_id = "truck-123"
        event_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

        result = await latch.check(source_id, event_time)

        assert result.decision == LatchDecision.PROCEED
        assert result.reason == "first_event"
        assert result.metadata["first_event"] is True

    @pytest.mark.asyncio
    async def test_in_order_event_proceeds(self, redis_mock):
        """Test that in-order events proceed."""
        latch = StatorLatch(redis_mock)
        source_id = "truck-123"
        event_time1 = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        event_time2 = datetime(2026, 5, 11, 12, 1, 0, tzinfo=UTC)

        # First event
        result1 = await latch.check(source_id, event_time1)
        assert result1.decision == LatchDecision.PROCEED

        # Second event (later time)
        result2 = await latch.check(source_id, event_time2)
        assert result2.decision == LatchDecision.PROCEED
        assert result2.reason == "event_time_after_last_committed"

    @pytest.mark.asyncio
    async def test_time_travel_detected(self, redis_mock):
        """Test that out-of-order events are detected as time-travel."""
        latch = StatorLatch(redis_mock)
        source_id = "truck-123"
        event_time1 = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        event_time2 = datetime(2026, 5, 11, 11, 0, 0, tzinfo=UTC)  # Earlier

        # First event
        result1 = await latch.check(source_id, event_time1)
        assert result1.decision == LatchDecision.PROCEED

        # Second event (earlier time - time travel!)
        result2 = await latch.check(source_id, event_time2)
        assert result2.decision == LatchDecision.BACKFILL
        assert result2.reason == "event_time_before_last_committed"
        assert result2.metadata["lag_seconds"] == 3600.0

    @pytest.mark.asyncio
    async def test_duplicate_detection(self, redis_mock):
        """Test duplicate detection within tolerance."""
        latch = StatorLatch(redis_mock)
        source_id = "truck-123"
        event_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        tolerance = 5  # 5 seconds

        # First event
        result1 = await latch.check(source_id, event_time, tolerance_seconds=tolerance)
        assert result1.decision == LatchDecision.PROCEED

        # Duplicate (same time)
        result2 = await latch.check(source_id, event_time, tolerance_seconds=tolerance)
        assert result2.decision == LatchDecision.DUPLICATE
        assert result2.reason == "duplicate_within_tolerance"

    @pytest.mark.asyncio
    async def test_reset_latch(self, redis_mock):
        """Test resetting the latch for an entity."""
        latch = StatorLatch(redis_mock)
        source_id = "truck-123"
        event_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

        # Set latch
        await latch.check(source_id, event_time)
        last_committed = await latch.get_last_committed_time(source_id)
        assert last_committed is not None

        # Reset
        await latch.reset(source_id)
        last_committed_after = await latch.get_last_committed_time(source_id)
        assert last_committed_after is None


class TestReorderBuffer:
    """Test Re-ordering Buffer for out-of-order events."""

    @pytest.fixture
    def redis_mock(self):
        """Return None to force in-memory buffer path in tests."""

    @pytest.mark.asyncio
    async def test_first_event_released_immediately(self, redis_mock):
        """Test that the first event for an entity is released immediately."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"
        event = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )

        should_release, released_event = await buffer.add(event, source_id, event.time)

        assert should_release is True
        assert released_event is event

    @pytest.mark.asyncio
    async def test_in_order_events_released(self, redis_mock):
        """Test that in-order events are released immediately."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"

        event1 = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )
        event2 = MandalaEvent(
            id="event-2",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 1, 0, tzinfo=UTC),
        )

        # First event
        should_release1, released1 = await buffer.add(event1, source_id, event1.time)
        assert should_release1 is True

        # Second event (in-order)
        should_release2, released2 = await buffer.add(event2, source_id, event2.time)
        assert should_release2 is True

    @pytest.mark.asyncio
    async def test_out_of_order_event_buffered(self, redis_mock):
        """Test that out-of-order events are buffered."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"

        event1 = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )
        event2 = MandalaEvent(
            id="event-2",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 11, 30, 0, tzinfo=UTC),  # Earlier
        )

        # First event
        await buffer.add(event1, source_id, event1.time)

        # Second event (out-of-order)
        should_release, released = await buffer.add(event2, source_id, event2.time)
        assert should_release is False
        assert released is None

    @pytest.mark.asyncio
    async def test_release_ready_events(self, redis_mock):
        """Test that release_ready returns buffered events that are ready."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"

        # Add event1 first (will be released immediately and set next_expected)
        event1 = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )
        await buffer.add(event1, source_id, event1.time)

        # Add event3 (out of order, will be buffered since there's a gap)
        event3 = MandalaEvent(
            id="event-3",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 1, 1, tzinfo=UTC),  # 61 seconds later (triggers gap detection)
        )
        await buffer.add(event3, source_id, event3.time)

        # Add event2 (fills the gap, should be released immediately)
        event2 = MandalaEvent(
            id="event-2",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 30, tzinfo=UTC),
        )
        await buffer.add(event2, source_id, event2.time)

        # Release ready events (event3 should now be released since event2 filled the gap)
        released = await buffer.release_ready(source_id)
        assert len(released) == 1
        assert released[0].id == "event-3"

    @pytest.mark.asyncio
    async def test_flush_all_events(self, redis_mock):
        """Test flushing all buffered events."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"

        event1 = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )
        event2 = MandalaEvent(
            id="event-2",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 11, 30, 0, tzinfo=UTC),
        )

        # Add events out of order (both will be buffered since event1 sets next_expected)
        await buffer.add(event1, source_id, event1.time)
        await buffer.add(event2, source_id, event2.time)

        # Flush all (only buffered events are flushed)
        flushed = await buffer.release_all(source_id)
        # Only event2 is buffered (event1 was released immediately as first event)
        assert len(flushed) == 1
        assert flushed[0].id == "event-2"

    @pytest.mark.asyncio
    async def test_get_stats(self, redis_mock):
        """Test getting buffer statistics."""
        buffer = ReorderBuffer(redis=redis_mock)
        source_id = "truck-123"

        event = MandalaEvent(
            id="event-1",
            source="test",
            type="test.event",
            time=datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
        )

        await buffer.add(event, source_id, event.time)
        stats = await buffer.get_stats()

        assert "total_buffered" in stats
        assert "total_released" in stats
        assert "active_entities" in stats


class TestReorderBufferManager:
    """Test ReorderBufferManager with background task."""

    @pytest.fixture
    def redis_mock(self):
        """Return None to force in-memory buffer path in tests."""

    @pytest.mark.asyncio
    async def test_start_stop(self, redis_mock):
        """Test starting and stopping the manager."""
        manager = ReorderBufferManager(redis=redis_mock)

        await manager.start(check_interval_seconds=0.1)
        assert manager._running is True

        await manager.stop()
        assert manager._running is False


class TestProcessTelemetryWithLatch:
    """Test the telemetry processing function with latch."""

    @pytest.fixture
    def redis_mock(self):
        """Mock Redis client for testing."""

        class MockRedis:
            def __init__(self):
                self.data = {}

            async def get(self, key):
                return self.data.get(key)

            async def set(self, key, value, ex=None):
                self.data[key] = value

        return MockRedis()

    @pytest.mark.asyncio
    async def test_process_in_order_telemetry(self, redis_mock):
        """Test processing in-order telemetry."""
        latch = StatorLatch(redis_mock)

        packet = {
            "source_id": "truck-123",
            "event_time": datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
            "latitude": 29.4267,
            "longitude": -98.4893,
        }

        result = await process_telemetry_with_latch(packet, latch)

        assert result.decision == LatchDecision.PROCEED

    @pytest.mark.asyncio
    async def test_process_out_of_order_telemetry(self, redis_mock):
        """Test processing out-of-order telemetry (time-travel)."""
        latch = StatorLatch(redis_mock)

        # First event
        packet1 = {
            "source_id": "truck-123",
            "event_time": datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
            "latitude": 29.4267,
            "longitude": -98.4893,
        }
        await process_telemetry_with_latch(packet1, latch)

        # Second event (earlier time)
        packet2 = {
            "source_id": "truck-123",
            "event_time": datetime(2026, 5, 11, 11, 0, 0, tzinfo=UTC),
            "latitude": 27.5060,
            "longitude": -99.4969,
        }
        result = await process_telemetry_with_latch(packet2, latch)

        assert result.decision == LatchDecision.BACKFILL


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
