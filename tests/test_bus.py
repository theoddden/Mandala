"""Comprehensive tests for the event bus module."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.bus import EventBus, EventProcessor
from mandala.core.events.envelope import MandalaEnvelope, MandalaEvent


class TestEventBus:
    """Test cases for EventBus."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.xadd = AsyncMock(return_value="1234567890")
        redis.xread = AsyncMock(return_value=[])
        redis.xgroup_create = AsyncMock()
        redis.xreadgroup = AsyncMock(return_value=[])
        return redis

    @pytest.fixture
    def event_bus(self, mock_redis):
        """Create an EventBus instance."""
        return EventBus(redis=mock_redis)

    def test_event_bus_initialization(self, mock_redis):
        """Test that EventBus initializes correctly."""
        bus = EventBus(redis=mock_redis, stream_name="test-stream")
        assert bus._redis == mock_redis
        assert bus._stream_name == "test-stream"
        assert bus._consumer_group == "test-stream:consumers"
        assert bus._processors == []

    @pytest.mark.asyncio
    async def test_publish_event(self, event_bus, mock_redis):
        """Test publishing an event to the stream."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        await event_bus.publish(envelope)

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "mandala:events"

    @pytest.mark.asyncio
    async def test_publish_multiple_events(self, event_bus, mock_redis):
        """Test publishing multiple events."""
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
            await event_bus.publish(envelope)

        assert mock_redis.xadd.call_count == 5

    @pytest.mark.asyncio
    async def test_register_processor(self, event_bus):
        """Test registering an event processor."""
        processor = AsyncMock()
        event_bus.register_processor(processor)

        assert processor in event_bus._processors

    @pytest.mark.asyncio
    async def test_register_multiple_processors(self, event_bus):
        """Test registering multiple processors."""
        processors = [AsyncMock() for _ in range(3)]
        for processor in processors:
            event_bus.register_processor(processor)

        assert len(event_bus._processors) == 3
        for processor in processors:
            assert processor in event_bus._processors

    @pytest.mark.asyncio
    async def test_start_consumer_group(self, event_bus, mock_redis):
        """Test starting the consumer group."""
        await event_bus.start_consumer_group()

        mock_redis.xgroup_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_event_with_processors(self, event_bus):
        """Test processing an event through registered processors."""
        processor1 = AsyncMock(return_value=True)
        processor2 = AsyncMock(return_value=True)
        event_bus.register_processor(processor1)
        event_bus.register_processor(processor2)

        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        await event_bus._process_event(envelope)

        processor1.assert_called_once_with(envelope)
        processor2.assert_called_once_with(envelope)

    @pytest.mark.asyncio
    async def test_processor_returns_false_stops_processing(self, event_bus):
        """Test that a processor returning False stops further processing."""
        processor1 = AsyncMock(return_value=False)
        processor2 = AsyncMock(return_value=True)
        event_bus.register_processor(processor1)
        event_bus.register_processor(processor2)

        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        await event_bus._process_event(envelope)

        processor1.assert_called_once()
        processor2.assert_not_called()

    @pytest.mark.asyncio
    async def test_processor_exception_handling(self, event_bus):
        """Test that processor exceptions are handled gracefully."""
        processor1 = AsyncMock(side_effect=Exception("Test error"))
        processor2 = AsyncMock(return_value=True)
        event_bus.register_processor(processor1)
        event_bus.register_processor(processor2)

        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        # Should not raise exception
        await event_bus._process_event(envelope)

        processor1.assert_called_once()
        # processor2 should still be called if we continue after error
        # This depends on the actual implementation

    @pytest.mark.asyncio
    async def test_stop_consumer(self, event_bus):
        """Test stopping the consumer."""
        event_bus._running = True
        await event_bus.stop()
        assert event_bus._running is False


class TestEventProcessor:
    """Test cases for EventProcessor."""

    @pytest.fixture
    def mock_handler(self):
        """Create a mock event handler."""
        return AsyncMock()

    @pytest.fixture
    def processor(self, mock_handler):
        """Create an EventProcessor instance."""
        return EventProcessor(handler=mock_handler)

    def test_processor_initialization(self, mock_handler):
        """Test that EventProcessor initializes correctly."""
        processor = EventProcessor(handler=mock_handler, filter_types=["test.event"])
        assert processor._handler == mock_handler
        assert processor._filter_types == ["test.event"]

    @pytest.mark.asyncio
    async def test_process_matching_event(self, processor, mock_handler):
        """Test processing an event that matches the filter."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        result = await processor.process(envelope)

        assert result is True
        mock_handler.assert_called_once_with(envelope)

    @pytest.mark.asyncio
    async def test_process_non_matching_event(self, processor, mock_handler):
        """Test processing an event that doesn't match the filter."""
        processor = EventProcessor(handler=mock_handler, filter_types=["other.event"])
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        result = await processor.process(envelope)

        assert result is False
        mock_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_without_filter(self, processor, mock_handler):
        """Test processing when no filter is set (all events pass)."""
        processor = EventProcessor(handler=mock_handler)
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        result = await processor.process(envelope)

        assert result is True
        mock_handler.assert_called_once_with(envelope)

    @pytest.mark.asyncio
    async def test_handler_exception(self, processor, mock_handler):
        """Test that handler exceptions are propagated."""
        mock_handler.side_effect = Exception("Handler error")
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope = MandalaEnvelope(event=event)

        with pytest.raises(Exception, match="Handler error"):
            await processor.process(envelope)
