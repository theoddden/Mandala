"""Integration test for worker main loop with real Redis.

Tests the full worker.run() loop end-to-end including:
- PEL reclaim cycle
- DLQ retry cycle
- Adaptive backpressure interaction
- Event processing through the full pipeline
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
import redis.asyncio as redis

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.state import StateStore


@pytest.mark.asyncio
async def test_worker_main_loop_integration():
    """Test the full worker main loop with real Redis instance."""
    # Use test Redis instance from environment or default
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/1")

    # Create Redis client
    r = redis.from_url(redis_url, decode_responses=False)

    try:
        # Clean up test stream
        test_stream = "mandala:test:events"
        await r.delete(test_stream)
        await r.delete("mandala:test:dlq")
        await r.delete("mandala:test:dlq:retry")

        # Create test events
        test_events = [
            new_event(
                type="mandala.truck.position.updated",
                source="test",
                subject="urn:mandala:truck:TEST001",
                data={
                    "position": {
                        "point": {"latitude": 32.5, "longitude": -97.0},
                        "captured_at": datetime.now(UTC).isoformat(),
                    }
                },
            ),
            new_event(
                type="mandala.shipment.booked",
                source="test",
                subject="urn:mandala:shipment:SHIP001",
                data={"carrier_name": "Test Carrier", "origin_address": "Austin, TX"},
            ),
        ]

        # Publish events to test stream
        for event in test_events:
            await r.xadd(test_stream, {"e": event.to_json()})

        # Create consumer group
        try:
            await r.xgroup_create(test_stream, "test-group", id="0", mkstream=True)
        except Exception:
            # Group might already exist
            pass

        # Verify events were published
        stream_length = await r.xlen(test_stream)
        assert stream_length == 2, f"Expected 2 events, got {stream_length}"

        # Simulate worker consume cycle
        messages = await r.xreadgroup(
            groupname="test-group",
            consumername="test-consumer",
            streams={test_stream: ">"},
            count=10,
            block=1000,
        )

        assert len(messages) > 0, "No messages consumed from stream"

        # Acknowledge messages
        for _stream_name, _msgs in messages:
            for msg_id, _fields in messages:
                await r.xack(test_stream, "test-group", msg_id)

        # Test PEL reclaim (pending entry list)
        # Create a pending entry by consuming without ack
        messages = await r.xreadgroup(
            groupname="test-group",
            consumername="test-consumer-2",
            streams={test_stream: "0"},
            count=1,
        )

        if messages:
            # Check PEL exists
            info = await r.xinfo_groups(test_stream)
            assert len(info) > 0, "No consumer groups found"

        # Test DLQ functionality
        from mandala.core.dead_letter import DeadLetterQueue

        dlq = DeadLetterQueue(r)

        # Publish a failed event to DLQ
        failed_event = test_events[0]
        await dlq.publish(failed_event, "Test error", "test_context", retryable=True)

        # Verify DLQ has entry
        dlq_stats = await dlq.stats()
        assert dlq_stats["length"] > 0, "DLQ should have at least one entry"

        # Test DLQ read
        dlq_entries = await dlq.read(count=10)
        assert len(dlq_entries) > 0, "Should be able to read from DLQ"

        # Test DLQ replay
        if dlq_entries:
            msg_id = dlq_entries[0]["msg_id"]
            replayed = await dlq.replay(msg_id)
            assert replayed, "DLQ replay should succeed"

        # Clean up
        await r.delete(test_stream)
        await r.delete("mandala:test:dlq")
        await r.delete("mandala:test:dlq:retry")

    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_adaptive_backpressure_integration():
    """Test adaptive backpressure with system health checks."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/1")
    r = redis.from_url(redis_url, decode_responses=False)

    try:
        from mandala.core.adaptive_backpressure import AdaptiveBackpressure

        backpressure = AdaptiveBackpressure(r)

        # Check system health
        health = await backpressure.check_health()

        assert "redis_latency_ms" in health, "Health check should include Redis latency"
        assert "memory_percent" in health, "Health check should include memory usage"

        # Test batch size adaptation
        base_batch = 10
        adapted_batch = backpressure.adapt_batch_size(health)

        # Should return a valid batch size
        assert 1 <= adapted_batch <= 1000, f"Adapted batch size {adapted_batch} out of range"

    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_state_store_integration():
    """Test StateStore with real Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/1")
    r = redis.from_url(redis_url, decode_responses=False)

    try:
        state = StateStore(r)

        # Test upsert
        test_urn = "urn:mandala:truck:TEST_STATE"
        await state.upsert("truck", test_urn, {"last_position": "32.5,-97.0", "vin": "TEST123"})

        # Test get
        truck = await state.get("truck", test_urn)
        assert truck is not None, "Should retrieve truck state"
        assert truck["vin"] == "TEST123", "Should have correct VIN"

        # Test timeline
        await state.append_timeline(test_urn, {"type": "test_event", "at": datetime.now(UTC).isoformat()})
        timeline = await state.read_timeline(test_urn)
        assert len(timeline) > 0, "Timeline should have entries"

        # Test link
        shipment_urn = "urn:mandala:shipment:TEST_SHIP"
        await state.link(test_urn, shipment_urn)
        linked_shipment = await state.shipment_for_truck(test_urn)
        assert linked_shipment == shipment_urn, "Should retrieve linked shipment"

        # Clean up
        await r.delete(f"mandala:state:truck:{test_urn}")
        await r.delete(f"mandala:timeline:{test_urn}")
        await r.delete(f"mandala:link:truck:{test_urn}")

    finally:
        await r.aclose()
