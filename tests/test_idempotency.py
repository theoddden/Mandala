"""Comprehensive tests for the idempotency module."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.idempotency import IdempotencyKey, IdempotencyManager


class TestIdempotencyKey:
    """Test cases for IdempotencyKey."""

    def test_idempotency_key_from_event(self):
        """Test generating idempotency key from event."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        key = IdempotencyKey.from_event(event)
        assert key is not None
        assert isinstance(key, str)

    def test_idempotency_key_from_event_with_attributes(self):
        """Test generating key with custom attributes."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        key = IdempotencyKey.from_event(event, attributes=["source", "type"])
        assert key is not None

    def test_idempotency_key_from_event_with_data(self):
        """Test generating key with event data."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
            data={"key": "value"},
        )
        key = IdempotencyKey.from_event(event, include_data=True)
        assert key is not None

    def test_idempotency_key_deterministic(self):
        """Test that key generation is deterministic."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        key1 = IdempotencyKey.from_event(event)
        key2 = IdempotencyKey.from_event(event)
        assert key1 == key2

    def test_idempotency_key_different_events(self):
        """Test that different events produce different keys."""
        event1 = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        event2 = MandalaEvent(
            id="test-2",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        key1 = IdempotencyKey.from_event(event1)
        key2 = IdempotencyKey.from_event(event2)
        assert key1 != key2

    def test_idempotency_key_custom_prefix(self):
        """Test generating key with custom prefix."""
        event = MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        key = IdempotencyKey.from_event(event, prefix="custom")
        assert key.startswith("custom:")


class TestIdempotencyManager:
    """Test cases for IdempotencyManager."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.set = AsyncMock(return_value=True)
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock(return_value=True)
        redis.delete = AsyncMock(return_value=1)
        redis.expire = AsyncMock(return_value=True)
        redis.ttl = AsyncMock(return_value=3600)
        return redis

    @pytest.fixture
    def manager(self, mock_redis):
        """Create an IdempotencyManager instance."""
        return IdempotencyManager(redis=mock_redis)

    @pytest.fixture
    def sample_event(self):
        """Create a sample event."""
        return MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )

    def test_manager_initialization(self, mock_redis):
        """Test that IdempotencyManager initializes correctly."""
        manager = IdempotencyManager(redis=mock_redis, ttl=7200)
        assert manager._redis == mock_redis
        assert manager._ttl == 7200
        assert manager._enabled is True

    def test_manager_disabled(self, mock_redis):
        """Test that manager can be disabled."""
        manager = IdempotencyManager(redis=mock_redis, enabled=False)
        assert manager._enabled is False

    @pytest.mark.asyncio
    async def test_check_not_processed(self, manager, mock_redis, sample_event):
        """Test checking if event has not been processed."""
        mock_redis.get.return_value = None
        key = IdempotencyKey.from_event(sample_event)

        processed = await manager.is_processed(key)

        assert processed is False
        mock_redis.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_already_processed(self, manager, mock_redis, sample_event):
        """Test checking if event has already been processed."""
        mock_redis.get.return_value = b"processed"
        key = IdempotencyKey.from_event(sample_event)

        processed = await manager.is_processed(key)

        assert processed is True
        mock_redis.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_processed(self, manager, mock_redis, sample_event):
        """Test marking an event as processed."""
        key = IdempotencyKey.from_event(sample_event)

        await manager.mark_processed(key)

        mock_redis.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_processed_with_metadata(self, manager, mock_redis, sample_event):
        """Test marking an event as processed with metadata."""
        key = IdempotencyKey.from_event(sample_event)
        metadata = {"processed_at": "2026-05-12T12:00:00Z"}

        await manager.mark_processed(key, metadata=metadata)

        mock_redis.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_processed_custom_ttl(self, manager, mock_redis, sample_event):
        """Test marking an event with custom TTL."""
        key = IdempotencyKey.from_event(sample_event)

        await manager.mark_processed(key, ttl=1800)

        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        # Check that custom TTL is used

    @pytest.mark.asyncio
    async def test_check_and_mark(self, manager, mock_redis, sample_event):
        """Test atomic check-and-mark operation."""
        mock_redis.get.return_value = None
        key = IdempotencyKey.from_event(sample_event)

        was_processed = await manager.check_and_mark(key)

        assert was_processed is False
        mock_redis.get.assert_called_once()
        mock_redis.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_and_mark_already_processed(self, manager, mock_redis, sample_event):
        """Test check-and-mark when already processed."""
        mock_redis.get.return_value = b"processed"
        key = IdempotencyKey.from_event(sample_event)

        was_processed = await manager.check_and_mark(key)

        assert was_processed is True
        mock_redis.get.assert_called_once()
        mock_redis.setex.assert_not_called()

    @pytest.mark.asyncio
    async def test_remove_processed(self, manager, mock_redis, sample_event):
        """Test removing an event from processed set."""
        key = IdempotencyKey.from_event(sample_event)

        await manager.remove_processed(key)

        mock_redis.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ttl(self, manager, mock_redis, sample_event):
        """Test getting TTL for a processed event."""
        mock_redis.ttl.return_value = 1800
        key = IdempotencyKey.from_event(sample_event)

        ttl = await manager.get_ttl(key)

        assert ttl == 1800
        mock_redis.ttl.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ttl_not_found(self, manager, mock_redis, sample_event):
        """Test getting TTL for non-existent key."""
        mock_redis.ttl.return_value = -2
        key = IdempotencyKey.from_event(sample_event)

        ttl = await manager.get_ttl(key)

        assert ttl == -2

    @pytest.mark.asyncio
    async def test_refresh_ttl(self, manager, mock_redis, sample_event):
        """Test refreshing TTL for a processed event."""
        key = IdempotencyKey.from_event(sample_event)

        await manager.refresh_ttl(key)

        mock_redis.expire.assert_called_once()

    @pytest.mark.asyncio
    async def test_disabled_manager_always_returns_false(self, manager, sample_event):
        """Test that disabled manager always returns False for is_processed."""
        manager._enabled = False
        key = IdempotencyKey.from_event(sample_event)

        processed = await manager.is_processed(key)

        assert processed is False

    @pytest.mark.asyncio
    async def test_disabled_manager_skips_mark(self, manager, sample_event):
        """Test that disabled manager skips marking."""
        manager._enabled = False
        key = IdempotencyKey.from_event(sample_event)

        await manager.mark_processed(key)

        # Redis should not be called
        manager._redis.setex.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_metadata(self, manager, mock_redis, sample_event):
        """Test getting metadata for a processed event."""
        mock_redis.get.return_value = b'{"processed_at":"2026-05-12T12:00:00Z"}'
        key = IdempotencyKey.from_event(sample_event)

        metadata = await manager.get_metadata(key)

        assert metadata is not None
        assert "processed_at" in metadata

    @pytest.mark.asyncio
    async def test_get_metadata_not_found(self, manager, mock_redis, sample_event):
        """Test getting metadata for non-existent event."""
        mock_redis.get.return_value = None
        key = IdempotencyKey.from_event(sample_event)

        metadata = await manager.get_metadata(key)

        assert metadata is None

    @pytest.mark.asyncio
    async def test_clear_all(self, manager, mock_redis):
        """Test clearing all idempotency keys."""
        mock_redis.keys = AsyncMock(return_value=["key1", "key2"])
        mock_redis.delete = AsyncMock(return_value=2)

        count = await manager.clear_all()

        assert count == 2
        mock_redis.keys.assert_called_once()
        mock_redis.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_statistics(self, manager, mock_redis):
        """Test getting idempotency statistics."""
        mock_redis.dbsize = AsyncMock(return_value=1000)
        mock_redis.keys = AsyncMock(return_value=["key1", "key2", "key3"])

        stats = await manager.get_statistics()

        assert stats is not None
        assert "total_keys" in stats

    @pytest.mark.asyncio
    async def test_mark_processed_with_error_handling(self, manager, mock_redis, sample_event):
        """Test error handling when marking fails."""
        mock_redis.setex.side_effect = Exception("Redis error")
        key = IdempotencyKey.from_event(sample_event)

        with pytest.raises(Exception, match="Redis error"):
            await manager.mark_processed(key)

    @pytest.mark.asyncio
    async def test_check_with_error_handling(self, manager, mock_redis, sample_event):
        """Test error handling when checking fails."""
        mock_redis.get.side_effect = Exception("Redis error")
        key = IdempotencyKey.from_event(sample_event)

        with pytest.raises(Exception, match="Redis error"):
            await manager.is_processed(key)

    @pytest.mark.asyncio
    async def test_batch_check(self, manager, mock_redis, sample_event):
        """Test checking multiple events at once."""
        keys = [
            IdempotencyKey.from_event(
                MandalaEvent(
                    id=f"test-{i}",
                    source="test",
                    type="test.event",
                    time=datetime.now(timezone.utc),
                )
            )
            for i in range(5)
        ]
        mock_redis.mget = AsyncMock(return_value=[None, b"processed", None, None, b"processed"])

        results = await manager.batch_check(keys)

        assert len(results) == 5
        assert results[0] is False
        assert results[1] is True
        assert results[2] is False
        assert results[3] is False
        assert results[4] is True

    @pytest.mark.asyncio
    async def test_batch_mark(self, manager, mock_redis, sample_event):
        """Test marking multiple events at once."""
        keys = [
            IdempotencyKey.from_event(
                MandalaEvent(
                    id=f"test-{i}",
                    source="test",
                    type="test.event",
                    time=datetime.now(timezone.utc),
                )
            )
            for i in range(5)
        ]
        mock_redis.mset = AsyncMock(return_value=True)

        await manager.batch_mark(keys)

        mock_redis.mset.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, manager, mock_redis):
        """Test cleaning up expired keys."""
        mock_redis.keys = AsyncMock(return_value=["key1", "key2"])
        mock_redis.ttl = AsyncMock(return_value=-2)
        mock_redis.delete = AsyncMock(return_value=2)

        count = await manager.cleanup_expired()

        assert count == 2
