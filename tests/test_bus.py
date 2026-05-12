"""Test event bus functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.events.envelope import MandalaEvent, new_event


@pytest.mark.asyncio
async def test_event_bus_protocol():
    """Test EventBus protocol definition."""
    from mandala.core.bus import EventBus

    # EventBus is a Protocol, so we can't instantiate it directly
    # Just verify it has the required method signatures
    assert hasattr(EventBus, "publish")
    assert hasattr(EventBus, "subscribe")
    assert hasattr(EventBus, "consume")
    assert hasattr(EventBus, "ack")


@pytest.mark.asyncio
async def test_redis_streams_bus_publish():
    """Test RedisStreamsBus publish method."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.script_load = AsyncMock(return_value="script-sha")
    mock_redis.evalsha = AsyncMock(return_value=1)  # Key added (new event)
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    event = new_event(type="test.event", source="test", subject="test-entity")
    message_id = await bus.publish("test-stream", event, enable_deduplication=True)

    assert message_id == "12345-0"
    mock_redis.script_load.assert_called_once()
    mock_redis.evalsha.assert_called_once()
    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_redis_streams_bus_publish_duplicate():
    """Test RedisStreamsBus publish with duplicate event."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.script_load = AsyncMock(return_value="script-sha")
    mock_redis.evalsha = AsyncMock(return_value=0)  # Key exists (duplicate)
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    event = new_event(type="test.event", source="test", subject="test-entity")
    message_id = await bus.publish("test-stream", event, enable_deduplication=True)

    # Should return empty string for duplicate
    assert message_id == ""
    mock_redis.script_load.assert_called_once()
    mock_redis.evalsha.assert_called_once()
    mock_redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_redis_streams_bus_publish_no_deduplication():
    """Test RedisStreamsBus publish without deduplication."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    event = new_event(type="test.event", source="test", subject="test-entity")
    message_id = await bus.publish("test-stream", event, enable_deduplication=False)

    assert message_id == "12345-0"
    mock_redis.script_load.assert_not_called()
    mock_redis.evalsha.assert_not_called()
    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_redis_streams_bus_publish_with_event_log():
    """Test RedisStreamsBus publish with Iceberg event log dual-write."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    mock_event_log = AsyncMock()
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis, event_log=mock_event_log)

    event = new_event(type="test.event", source="test", subject="test-entity")
    message_id = await bus.publish("test-stream", event, enable_deduplication=False)

    assert message_id == "12345-0"
    mock_redis.xadd.assert_called_once()
    # Event log append should be scheduled as background task


@pytest.mark.asyncio
async def test_redis_streams_bus_consume():
    """Test RedisStreamsBus consume method."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xreadgroup = AsyncMock(return_value=[
        (b"test-stream", [(b"12345-0", {b"e": b'{"type":"test.event","source":"test","data":{"test":"value"}}'})])
    ])
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    events = await bus.consume("test-stream", group="test-group", consumer="test-consumer", count=10)

    assert len(events) == 1
    assert events[0][0] == "12345-0"
    assert events[0][1].type == "test.event"
    mock_redis.xgroup_create.assert_called_once()
    mock_redis.xreadgroup.assert_called_once()


@pytest.mark.asyncio
async def test_redis_streams_bus_consume_empty():
    """Test RedisStreamsBus consume with no events."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xreadgroup = AsyncMock(return_value=None)
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    events = await bus.consume("test-stream", group="test-group", consumer="test-consumer", count=10)

    assert len(events) == 0


@pytest.mark.asyncio
async def test_redis_streams_bus_consume_malformed():
    """Test RedisStreamsBus consume with malformed event."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xack = AsyncMock()
    mock_redis.xreadgroup = AsyncMock(return_value=[
        ("test-stream", [(b"12345-0", {b"e": b"invalid json"})])
    ])
    mock_settings = Mock()
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.bus.get_settings", return_value=mock_settings):
        bus = RedisStreamsBus(mock_redis)

    events = await bus.consume("test-stream", group="test-group", consumer="test-consumer", count=10)

    assert len(events) == 0  # Malformed event should be skipped
    mock_redis.xack.assert_called_once()


@pytest.mark.asyncio
async def test_redis_streams_bus_ack():
    """Test RedisStreamsBus ack method."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xack = AsyncMock(return_value=1)

    bus = RedisStreamsBus(mock_redis)

    await bus.ack("test-stream", "test-group", "12345-0")

    mock_redis.xack.assert_called_once_with("test-stream", "test-group", "12345-0")


@pytest.mark.asyncio
async def test_redis_streams_bus_ensure_group():
    """Test RedisStreamsBus _ensure_group method."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()

    bus = RedisStreamsBus(mock_redis)

    await bus._ensure_group("test-stream", "test-group")

    mock_redis.xgroup_create.assert_called_once()


@pytest.mark.asyncio
async def test_redis_streams_bus_ensure_group_busygroup():
    """Test RedisStreamsBus _ensure_group with BUSYGROUP error."""
    from mandala.core.bus import RedisStreamsBus

    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock(side_effect=Exception("BUSYGROUP"))

    bus = RedisStreamsBus(mock_redis)

    # Should not raise on BUSYGROUP
    await bus._ensure_group("test-stream", "test-group")

    mock_redis.xgroup_create.assert_called_once()


@pytest.mark.asyncio
async def test_event_processor_filter():
    """Test EventProcessor with filter."""
    from mandala.core.bus import EventProcessor

    handler = AsyncMock(return_value=True)
    processor = EventProcessor(handler, filter_types=["test.event"])

    event = new_event(type="test.event", source="test", subject="test-entity")
    result = await processor.process(event)

    assert result is True
    handler.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_event_processor_filter_out():
    """Test EventProcessor filtering out non-matching events."""
    from mandala.core.bus import EventProcessor

    handler = AsyncMock(return_value=True)
    processor = EventProcessor(handler, filter_types=["test.event"])

    event = new_event(type="other.event", source="test", subject="test-entity")
    result = await processor.process(event)

    assert result is False
    handler.assert_not_called()


@pytest.mark.asyncio
async def test_event_processor_no_filter():
    """Test EventProcessor without filter (processes all events)."""
    from mandala.core.bus import EventProcessor

    handler = AsyncMock(return_value=True)
    processor = EventProcessor(handler, filter_types=None)

    event = new_event(type="any.event", source="test", subject="test-entity")
    result = await processor.process(event)

    assert result is True
    handler.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_event_processor_handler_none():
    """Test EventProcessor with None handler."""
    from mandala.core.bus import EventProcessor

    processor = EventProcessor(None, filter_types=None)

    event = new_event(type="test.event", source="test", subject="test-entity")
    result = await processor.process(event)

    assert result is True


def test_dedupe_script():
    """Test the deduplication Lua script."""
    from mandala.core.bus import DEDUPE_SCRIPT

    assert "local key = KEYS[1]" in DEDUPE_SCRIPT
    assert "local ttl = ARGV[1]" in DEDUPE_SCRIPT
    assert "EXISTS" in DEDUPE_SCRIPT
    assert "SETEX" in DEDUPE_SCRIPT
