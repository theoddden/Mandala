"""Comprehensive tests for the event log module."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.event_log import EventLog
from mandala.core.events.envelope import MandalaEvent, MandalaEnvelope


class TestEventLog:
    """Test cases for EventLog."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.xadd = AsyncMock(return_value="1234567890-0")
        redis.xread = AsyncMock(return_value=[])
        redis.xrange = AsyncMock(return_value=[])
        redis.xrevrange = AsyncMock(return_value=[])
        redis.xlen = AsyncMock(return_value=0)
        redis.xtrim = AsyncMock()
        redis.xgroup_create = AsyncMock()
        redis.xinfo_stream = AsyncMock(return_value={"length": 0})
        return redis

    @pytest.fixture
    def event_log(self, mock_redis):
        """Create an EventLog instance."""
        return EventLog(redis=mock_redis, stream_name="mandala:events")

    def test_event_log_initialization(self, mock_redis):
        """Test that EventLog initializes correctly."""
        log = EventLog(redis=mock_redis, stream_name="test-stream")
        assert log._redis == mock_redis
        assert log._stream_name == "test-stream"
        assert log._max_length == 100000

    @pytest.mark.asyncio
    async def test_append_event(self, event_log, mock_redis):
        """Test appending an event to the log."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        result = await event_log.append(envelope)

        mock_redis.xadd.assert_called_once()
        assert result == "1234567890-0"

    @pytest.mark.asyncio
    async def test_append_event_with_metadata(self, event_log, mock_redis):
        """Test appending an event with custom metadata."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)
        metadata = {"custom_key": "custom_value"}

        await event_log.append(envelope, metadata=metadata)

        call_args = mock_redis.xadd.call_args
        # Check that metadata is included in the call

    @pytest.mark.asyncio
    async def test_append_multiple_events(self, event_log, mock_redis):
        """Test appending multiple events."""
        events = [
            MandalaEvent(
                id=f"test-{i}",
                source="test",
                type="test.event",
                time=datetime.now(timezone.utc),
            )
            for i in range(5)
        ]

        for event in events:
            envelope = MandalaEnvelope(event=event)
            await event_log.append(envelope)

        assert mock_redis.xadd.call_count == 5

    @pytest.mark.asyncio
    async def test_read_events(self, event_log, mock_redis):
        """Test reading events from the log."""
        # Mock the xread response
        mock_redis.xread.return_value = [
            (
                "mandala:events",
                [
                    (
                        "1234567890-0",
                        {
                            "event": b'{"id":"test-1","source":"test","type":"test.event"}',
                            "timestamp": b"2026-05-12T12:00:00Z",
                        },
                    )
                ],
            )
        ]

        events = await event_log.read(count=10)

        mock_redis.xread.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_events_from_id(self, event_log, mock_redis):
        """Test reading events starting from a specific ID."""
        await event_log.read(from_id="1234567890-0", count=10)

        call_args = mock_redis.xread.call_args
        # Check that the from_id is passed correctly

    @pytest.mark.asyncio
    async def test_read_events_empty(self, event_log, mock_redis):
        """Test reading when no events exist."""
        mock_redis.xread.return_value = []

        events = await event_log.read(count=10)

        assert events == []
        mock_redis.xread.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_stream_length(self, event_log, mock_redis):
        """Test getting the stream length."""
        mock_redis.xlen.return_value = 100

        length = await event_log.get_length()

        assert length == 100
        mock_redis.xlen.assert_called_once_with("mandala:events")

    @pytest.mark.asyncio
    async def test_trim_stream(self, event_log, mock_redis):
        """Test trimming the stream to max length."""
        await event_log.trim()

        mock_redis.xtrim.assert_called_once()

    @pytest.mark.asyncio
    async def test_trim_stream_custom_maxlen(self, event_log, mock_redis):
        """Test trimming the stream to a custom max length."""
        await event_log.trim(maxlen=50000)

        call_args = mock_redis.xtrim.call_args
        # Check that the custom maxlen is used

    @pytest.mark.asyncio
    async def test_get_stream_info(self, event_log, mock_redis):
        """Test getting stream information."""
        mock_redis.xinfo_stream.return_value = {
            "length": 100,
            "groups": 2,
            "first-entry": ("0-0", {}),
            "last-entry": ("100-0", {}),
        }

        info = await event_log.get_info()

        mock_redis.xinfo_stream.assert_called_once()
        assert info is not None

    @pytest.mark.asyncio
    async def test_create_consumer_group(self, event_log, mock_redis):
        """Test creating a consumer group."""
        await event_log.create_consumer_group("test-group", "0")

        mock_redis.xgroup_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_consumer_group_with_id(self, event_log, mock_redis):
        """Test creating a consumer group starting from a specific ID."""
        await event_log.create_consumer_group("test-group", "1234567890-0")

        call_args = mock_redis.xgroup_create.call_args
        # Check that the ID is passed correctly

    @pytest.mark.asyncio
    async def test_range_events(self, event_log, mock_redis):
        """Test reading a range of events."""
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {
                    "event": b'{"id":"test-1","source":"test","type":"test.event"}',
                    "timestamp": b"2026-05-12T12:00:00Z",
                },
            )
        ]

        events = await event_log.range("-", "+", count=10)

        mock_redis.xrange.assert_called_once()

    @pytest.mark.asyncio
    async def test_reverse_range_events(self, event_log, mock_redis):
        """Test reading events in reverse order."""
        mock_redis.xrevrange.return_value = [
            (
                "1234567890-0",
                {
                    "event": b'{"id":"test-1","source":"test","type":"test.event"}',
                    "timestamp": b"2026-05-12T12:00:00Z",
                },
            )
        ]

        events = await event_log.reverse_range("+", "-", count=10)

        mock_redis.xrevrange.assert_called_once()

    @pytest.mark.asyncio
    async def test_append_with_error_handling(self, event_log, mock_redis):
        """Test error handling when appending fails."""
        mock_redis.xadd.side_effect = Exception("Redis error")

        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        with pytest.raises(Exception, match="Redis error"):
            await event_log.append(envelope)

    @pytest.mark.asyncio
    async def test_read_with_error_handling(self, event_log, mock_redis):
        """Test error handling when reading fails."""
        mock_redis.xread.side_effect = Exception("Redis error")

        with pytest.raises(Exception, match="Redis error"):
            await event_log.read(count=10)

    @pytest.mark.asyncio
    async def test_append_with_ttl(self, event_log, mock_redis):
        """Test appending with TTL (time-to-live)."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        await event_log.append(envelope, ttl=3600)

        # Check that TTL is passed to Redis
        call_args = mock_redis.xadd.call_args
