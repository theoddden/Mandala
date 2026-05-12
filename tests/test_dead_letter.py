"""Test dead letter queue functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.events.envelope import MandalaEvent, new_event


@pytest.mark.asyncio
async def test_dead_letter_queue_init():
    """Test DeadLetterQueue initialization."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    dlq = DeadLetterQueue(mock_redis)

    assert dlq._redis == mock_redis
    assert dlq._stream == "mandala:dlq"
    assert dlq._maxlen == 10_000
    assert dlq._retry_stream == "mandala:dlq:retry"


@pytest.mark.asyncio
async def test_dead_letter_queue_publish():
    """Test DeadLetterQueue publish method."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    dlq = DeadLetterQueue(mock_redis)

    event = new_event(type="test.event", source="test", subject="test-entity")
    await dlq.publish(event, error="Test error", context="detector")

    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_publish_dict():
    """Test DeadLetterQueue publish with dict event."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    dlq = DeadLetterQueue(mock_redis)

    event_dict = {"id": "test-id", "type": "test.event", "source": "test"}
    await dlq.publish(event_dict, error="Test error", context="detector")

    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_publish_retryable():
    """Test DeadLetterQueue publish with retryable flag."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="12345-0")
    dlq = DeadLetterQueue(mock_redis)

    event = new_event(type="test.event", source="test", subject="test-entity")
    await dlq.publish(event, error="Test error", context="detector", retryable=True)

    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_read():
    """Test DeadLetterQueue read method."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrevrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id"}, "error": "test"}).encode()})
    ])
    dlq = DeadLetterQueue(mock_redis)

    entries = await dlq.read(count=10)

    assert len(entries) == 1
    assert entries[0]["msg_id"] == "12345-0"
    mock_redis.xrevrange.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_read_empty():
    """Test DeadLetterQueue read with no entries."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrevrange = AsyncMock(return_value=[])
    dlq = DeadLetterQueue(mock_redis)

    entries = await dlq.read(count=10)

    assert len(entries) == 0


@pytest.mark.asyncio
async def test_dead_letter_queue_replay():
    """Test DeadLetterQueue replay method."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id", "type": "test.event", "source": "test"}}).encode()})
    ])
    mock_redis.xdel = AsyncMock()
    mock_settings = Mock()
    mock_settings.stream_inbound = "mandala:inbound"
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.dead_letter.get_settings", return_value=mock_settings), patch(
        "mandala.core.bus.RedisStreamsBus"
    ) as MockBus:
        mock_bus = AsyncMock()
        mock_bus.publish = AsyncMock(return_value="msg-id")
        MockBus.return_value = mock_bus

        dlq = DeadLetterQueue(mock_redis)
        result = await dlq.replay("12345-0")

        assert result is True
        mock_bus.publish.assert_called_once()
        mock_redis.xdel.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_replay_not_found():
    """Test DeadLetterQueue replay with non-existent message."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrange = AsyncMock(return_value=[])
    dlq = DeadLetterQueue(mock_redis)

    result = await dlq.replay("12345-0")

    assert result is False


@pytest.mark.asyncio
async def test_dead_letter_queue_delete():
    """Test DeadLetterQueue delete method."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xdel = AsyncMock(return_value=1)
    dlq = DeadLetterQueue(mock_redis)

    result = await dlq.delete("12345-0")

    assert result is True
    mock_redis.xdel.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_queue_stats():
    """Test DeadLetterQueue stats method."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xlen = AsyncMock(return_value=5)
    mock_redis.xrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id"}, "failed_at": "2024-01-01"}).encode()})
    ])
    dlq = DeadLetterQueue(mock_redis)

    stats = await dlq.stats()

    assert stats["length"] == 5
    assert stats["maxlen"] == 10_000
    assert stats["oldest_entry"] == "2024-01-01"
    assert stats["utilization"] == 5 / 10_000


@pytest.mark.asyncio
async def test_dead_letter_queue_stats_error():
    """Test DeadLetterQueue stats with error."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xlen = AsyncMock(side_effect=Exception("Redis error"))
    dlq = DeadLetterQueue(mock_redis)

    stats = await dlq.stats()

    assert "error" in stats


def test_calculate_backoff():
    """Test _calculate_backoff method."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    dlq = DeadLetterQueue(mock_redis)

    delay = dlq._calculate_backoff(0)
    assert delay >= 1.0  # base_delay

    delay = dlq._calculate_backoff(1)
    assert delay >= 1.0  # base_delay with jitter

    delay = dlq._calculate_backoff(10)
    # Jitter can cause delay to exceed max_delay temporarily, but should be close
    assert delay >= 1.0


@pytest.mark.asyncio
async def test_schedule_retry():
    """Test schedule_retry method."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id"}, "retryable": True, "retry_count": 0}).encode()})
    ])
    mock_redis.zadd = AsyncMock()
    mock_redis.xdel = AsyncMock()
    mock_redis.xadd = AsyncMock()
    dlq = DeadLetterQueue(mock_redis)

    result = await dlq.schedule_retry("12345-0")

    assert result is True
    mock_redis.zadd.assert_called_once()
    mock_redis.xdel.assert_called_once()


@pytest.mark.asyncio
async def test_schedule_retry_not_retryable():
    """Test schedule_retry with non-retryable event."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id"}, "retryable": False}).encode()})
    ])
    dlq = DeadLetterQueue(mock_redis)

    result = await dlq.schedule_retry("12345-0")

    assert result is False


@pytest.mark.asyncio
async def test_schedule_retry_not_found():
    """Test schedule_retry with non-existent message."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.xrange = AsyncMock(return_value=[])
    dlq = DeadLetterQueue(mock_redis)

    result = await dlq.schedule_retry("12345-0")

    assert result is False


@pytest.mark.asyncio
async def test_process_retries():
    """Test process_retries method."""
    import json

    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"12345-0"])
    mock_redis.xrange = AsyncMock(return_value=[
        (b"12345-0", {b"entry": json.dumps({"event": {"id": "test-id", "type": "test.event", "source": "test"}}).encode()})
    ])
    mock_redis.zrem = AsyncMock()
    mock_redis.xdel = AsyncMock()
    mock_settings = Mock()
    mock_settings.stream_inbound = "mandala:inbound"
    mock_settings.stream_maxlen = 10000

    with patch("mandala.core.dead_letter.get_settings", return_value=mock_settings), patch(
        "mandala.core.bus.RedisStreamsBus"
    ) as MockBus:
        mock_bus = AsyncMock()
        mock_bus.publish = AsyncMock(return_value="msg-id")
        MockBus.return_value = mock_bus

        dlq = DeadLetterQueue(mock_redis)
        count = await dlq.process_retries()

        assert count == 1
        mock_redis.zrem.assert_called_once()


@pytest.mark.asyncio
async def test_process_retries_empty():
    """Test process_retries with no due retries."""
    from mandala.core.dead_letter import DeadLetterQueue

    mock_redis = AsyncMock()
    mock_redis.zrangebyscore = AsyncMock(return_value=[])
    dlq = DeadLetterQueue(mock_redis)

    count = await dlq.process_retries()

    assert count == 0
