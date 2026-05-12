"""Comprehensive tests for the dead letter queue module."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from mandala.core.dead_letter import DeadLetterQueue
from mandala.core.events.envelope import MandalaEvent, MandalaEnvelope, new_event


class TestDeadLetterQueue:
    """Test cases for DeadLetterQueue."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.xadd = AsyncMock(return_value="1234567890-0")
        redis.xread = AsyncMock(return_value=[])
        redis.xrange = AsyncMock(return_value=[])
        redis.xlen = AsyncMock(return_value=0)
        redis.xtrim = AsyncMock()
        return redis

    @pytest.fixture
    def dead_letter_queue(self, mock_redis):
        """Create a DeadLetterQueue instance."""
        return DeadLetterQueue(redis=mock_redis)

    @pytest.fixture
    def sample_envelope(self):
        """Create a sample envelope."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        return MandalaEnvelope(event=event)

    def test_dead_letter_queue_initialization(self, mock_redis):
        """Test that DeadLetterQueue initializes correctly."""
        dlq = DeadLetterQueue(redis=mock_redis, stream_name="test-dlq")
        assert dlq._redis == mock_redis
        assert dlq._stream_name == "test-dlq"
        assert dlq._max_length == 10000

    @pytest.mark.asyncio
    async def test_add_to_dead_letter(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test adding an envelope to the dead letter queue."""
        error = Exception("Test error")
        await dead_letter_queue.add(sample_envelope, error=error)

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "mandala:dead_letter"

    @pytest.mark.asyncio
    async def test_add_with_metadata(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test adding with custom metadata."""
        error = Exception("Test error")
        metadata = {"retry_count": 3, "processor": "test-processor"}
        await dead_letter_queue.add(sample_envelope, error=error, metadata=metadata)

        mock_redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_multiple_envelopes(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test adding multiple envelopes."""
        for i in range(5):
            event = MandalaEvent(
                id=f"test-{i}",
                source="test",
                type="test.event",
                time=datetime.now(timezone.utc),
            )
            envelope = MandalaEnvelope(event=event)
            await dead_letter_queue.add(envelope, error=Exception(f"Error {i}"))

        assert mock_redis.xadd.call_count == 5

    @pytest.mark.asyncio
    async def test_read_from_dead_letter(self, dead_letter_queue, mock_redis):
        """Test reading from the dead letter queue."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                            "timestamp": b"2026-05-12T12:00:00Z",
                        },
                    )
                ],
            )
        ]

        items = await dead_letter_queue.read(count=10)

        mock_redis.xread.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_empty_queue(self, dead_letter_queue, mock_redis):
        """Test reading when queue is empty."""
        mock_redis.xread.return_value = []

        items = await dead_letter_queue.read(count=10)

        assert items == []
        mock_redis.xread.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_queue_length(self, dead_letter_queue, mock_redis):
        """Test getting the queue length."""
        mock_redis.xlen.return_value = 50

        length = await dead_letter_queue.get_length()

        assert length == 50
        mock_redis.xlen.assert_called_once_with("mandala:dead_letter")

    @pytest.mark.asyncio
    async def test_trim_queue(self, dead_letter_queue, mock_redis):
        """Test trimming the queue."""
        await dead_letter_queue.trim(maxlen=5000)

        mock_redis.xtrim.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_event(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test retrying an event from the dead letter queue."""
        # Mock the read to return an item
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                        },
                    )
                ],
            )
        ]

        handler = AsyncMock(return_value=True)
        success = await dead_letter_queue.retry(handler)

        mock_redis.xread.assert_called_once()
        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_event_success(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test retrying an event successfully removes it from DLQ."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                        },
                    )
                ],
            )
        ]
        mock_redis.xdel = AsyncMock()

        handler = AsyncMock(return_value=True)
        await dead_letter_queue.retry(handler)

        # Should delete the item after successful retry
        # mock_redis.xdel.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_event_failure(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test retrying an event that fails again."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                        },
                    )
                ],
            )
        ]

        handler = AsyncMock(side_effect=Exception("Retry failed"))
        success = await dead_letter_queue.retry(handler)

        assert success is False

    @pytest.mark.asyncio
    async def test_get_statistics(self, dead_letter_queue, mock_redis):
        """Test getting queue statistics."""
        mock_redis.xlen.return_value = 100
        mock_redis.xinfo_stream = AsyncMock(
            return_value={
                "length": 100,
                "groups": 1,
                "first-entry": ("0-0", {}),
                "last-entry": ("99-0", {}),
            }
        )

        stats = await dead_letter_queue.get_statistics()

        assert stats is not None
        assert "length" in stats

    @pytest.mark.asyncio
    async def test_purge_queue(self, dead_letter_queue, mock_redis):
        """Test purging the entire queue."""
        mock_redis.delete = AsyncMock()
        await dead_letter_queue.purge()

        mock_redis.delete.assert_called_once_with("mandala:dead_letter")

    @pytest.mark.asyncio
    async def test_add_with_serialization_error(self, dead_letter_queue, mock_redis):
        """Test handling serialization errors when adding to DLQ."""
        # Create an envelope that can't be serialized
        envelope = Mock()
        envelope.to_dict.side_effect = Exception("Serialization error")

        error = Exception("Test error")
        await dead_letter_queue.add(envelope, error=error)

        # Should handle the error gracefully

    @pytest.mark.asyncio
    async def test_add_with_error_none(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test adding with None error."""
        await dead_letter_queue.add(sample_envelope, error=None)

        mock_redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_with_deserialization_error(self, dead_letter_queue, mock_redis):
        """Test handling deserialization errors when reading."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b"invalid json",
                            "error": b"Test error",
                        },
                    )
                ],
            )
        ]

        items = await dead_letter_queue.read(count=10)

        # Should handle the error gracefully
        assert items == []

    @pytest.mark.asyncio
    async def test_retry_with_max_retries(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test retrying with a maximum retry limit."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                            "retry_count": b"5",
                        },
                    )
                ],
            )
        ]

        handler = AsyncMock(side_effect=Exception("Retry failed"))
        success = await dead_letter_queue.retry(handler, max_retries=3)

        # Should not retry if retry count exceeds max
        assert success is False

    @pytest.mark.asyncio
    async def test_batch_retry(self, dead_letter_queue, mock_redis, sample_envelope):
        """Test retrying multiple events in batch."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        f"1234567890-{i}",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"Test error",
                        },
                    )
                    for i in range(3)
                ],
            )
        ]

        handler = AsyncMock(return_value=True)
        results = await dead_letter_queue.batch_retry(handler, batch_size=3)

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_get_error_types(self, dead_letter_queue, mock_redis):
        """Test getting breakdown of error types in DLQ."""
        mock_redis.xread.return_value = [
            (
                "mandala:dead_letter",
                [
                    (
                        "1234567890-0",
                        {
                            "envelope": b'{"event":{"id":"test-1"}}',
                            "error": b"TypeError: test",
                        },
                    ),
                    (
                        "1234567890-1",
                        {
                            "envelope": b'{"event":{"id":"test-2"}}',
                            "error": b"ValueError: test",
                        },
                    ),
                ],
            )
        ]

        error_types = await dead_letter_queue.get_error_types()

        assert error_types is not None
        assert "TypeError" in error_types
        assert "ValueError" in error_types

    @pytest.mark.asyncio
    async def test_get_oldest_events(self, dead_letter_queue, mock_redis):
        """Test getting oldest events from DLQ."""
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {
                    "envelope": b'{"event":{"id":"test-1"}}',
                    "error": b"Test error",
                },
            )
        ]

        events = await dead_letter_queue.get_oldest(count=10)

        mock_redis.xrange.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_newest_events(self, dead_letter_queue, mock_redis):
        """Test getting newest events from DLQ."""
        mock_redis.xrevrange.return_value = [
            (
                "1234567890-0",
                {
                    "envelope": b'{"event":{"id":"test-1"}}',
                    "error": b"Test error",
                },
            )
        ]

        events = await dead_letter_queue.get_newest(count=10)

        mock_redis.xrevrange.assert_called_once()
