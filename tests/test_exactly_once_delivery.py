"""Test exactly-once delivery with idempotency keys.

This test verifies that duplicate events are correctly deduplicated
at the stream level using the SHA256-derived idempotency key.
"""
import asyncio
from datetime import UTC, datetime

import pytest
import redis.asyncio as redis

from mandala.core.bus import RedisStreamsBus
from mandala.core.events.envelope import MandalaEvent, new_event


@pytest.mark.asyncio
async def test_idempotency_key_generation():
    """Test that idempotency keys are deterministic."""
    # Same logical event (vendor, type, time, subject) must produce the same key.
    fixed_time = datetime.now(UTC)
    event1 = new_event(
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    event1.time = fixed_time

    event2 = new_event(
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    event2.time = fixed_time
    
    # Same event should produce same key
    key1 = event1.compute_idempotency_key()
    key2 = event2.compute_idempotency_key()
    assert key1 == key2
    
    # Different event should produce different key
    event3 = new_event(
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        subject="truck:67890",
        data={"truck_id": "67890", "latitude": 32.7157, "longitude": -117.1611},
    )
    key3 = event3.compute_idempotency_key()
    assert key1 != key3


@pytest.mark.asyncio
async def test_duplicate_event_deduplication():
    """Test that duplicate events are dropped at stream level."""
    # Create Redis client
    redis_client = await redis.from_url("redis://localhost:6379/0", decode_responses=False)
    bus = RedisStreamsBus(redis_client)
    
    # Create identical events
    event1 = MandalaEvent(
        id="test-1",
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        time=datetime.now(UTC),
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    
    event2 = MandalaEvent(
        id="test-2",  # Different ID but same payload
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        time=event1.time,  # Same time for deterministic key
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    
    # Compute idempotency keys
    key1 = event1.compute_idempotency_key()
    key2 = event2.compute_idempotency_key()
    assert key1 == key2
    
    # Publish first event
    msg_id1 = await bus.publish("test:stream", event1, enable_deduplication=True)
    assert msg_id1 != ""  # Should succeed

    # Publish duplicate event
    msg_id2 = await bus.publish("test:stream", event2, enable_deduplication=True)
    assert msg_id2 == ""  # Should be dropped (empty string indicates duplicate)
    
    # Cleanup
    await redis_client.delete("mandala:idempotency:" + key1)
    await redis_client.delete("test:stream")
    await redis_client.aclose()


@pytest.mark.asyncio
async def test_deduplication_can_be_disabled():
    """Test that deduplication can be disabled."""
    redis_client = await redis.from_url("redis://localhost:6379/0", decode_responses=False)
    bus = RedisStreamsBus(redis_client)
    
    event1 = MandalaEvent(
        id="test-1",
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        time=datetime.now(UTC),
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    
    event2 = MandalaEvent(
        id="test-2",
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        time=event1.time,
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    
    # Publish first event with deduplication enabled
    msg_id1 = await bus.publish("test:stream2", event1, enable_deduplication=True)
    assert msg_id1 != ""

    # Publish duplicate event with deduplication DISABLED
    msg_id2 = await bus.publish("test:stream2", event2, enable_deduplication=False)
    assert msg_id2 != ""  # Should succeed (deduplication disabled)
    
    # Cleanup
    key = event1.compute_idempotency_key()
    await redis_client.delete("mandala:idempotency:" + key)
    await redis_client.delete("test:stream2")
    await redis_client.aclose()


@pytest.mark.asyncio
async def test_idempotency_key_ttl():
    """Test that idempotency keys expire after TTL."""
    redis_client = await redis.from_url("redis://localhost:6379/0", decode_responses=False)
    bus = RedisStreamsBus(redis_client)
    
    event = MandalaEvent(
        id="test-1",
        type="mandala.truck.location",
        source="mandala/connector/samsara",
        time=datetime.now(UTC),
        subject="truck:12345",
        data={"truck_id": "12345", "latitude": 32.7157, "longitude": -117.1611},
    )
    
    key = event.compute_idempotency_key()
    
    # Publish event
    msg_id1 = await bus.publish("test:stream3", event, enable_deduplication=True)
    assert msg_id1 != ""
    
    # Verify key exists in Redis
    ttl = await redis_client.ttl("mandala:idempotency:" + key)
    assert ttl > 0  # Key should have TTL set
    
    # Cleanup
    await redis_client.delete("mandala:idempotency:" + key)
    await redis_client.delete("test:stream3")
    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(test_idempotency_key_generation())
    asyncio.run(test_duplicate_event_deduplication())
    asyncio.run(test_deduplication_can_be_disabled())
    asyncio.run(test_idempotency_key_ttl())
    print("All tests passed!")
