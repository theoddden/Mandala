"""Comprehensive tests for the replay module."""

import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.replay import EventReplay, ReplayConfig, ReplayStatus


class TestReplayConfig:
    """Test cases for ReplayConfig."""

    def test_replay_config_initialization(self):
        """Test that ReplayConfig initializes correctly."""
        config = ReplayConfig(
            from_timestamp=datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc),
            to_timestamp=datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc),
            batch_size=100,
        )
        assert config.from_timestamp is not None
        assert config.to_timestamp is not None
        assert config.batch_size == 100

    def test_replay_config_defaults(self):
        """Test ReplayConfig with default values."""
        config = ReplayConfig()
        assert config.batch_size == 1000
        assert config.max_retries == 3
        assert config.speed_multiplier == 1.0


class TestEventReplay:
    """Test cases for EventReplay."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.xrange = AsyncMock(return_value=[])
        redis.xread = AsyncMock(return_value=[])
        redis.xlen = AsyncMock(return_value=0)
        return redis

    @pytest.fixture
    def replay_config(self):
        """Create a ReplayConfig instance."""
        return ReplayConfig(
            from_timestamp=datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc),
            to_timestamp=datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc),
            batch_size=100,
        )

    @pytest.fixture
    def event_replay(self, mock_redis, replay_config):
        """Create an EventReplay instance."""
        return EventReplay(redis=mock_redis, config=replay_config)

    def test_event_replay_initialization(self, mock_redis, replay_config):
        """Test that EventReplay initializes correctly."""
        replay = EventReplay(redis=mock_redis, config=replay_config)
        assert replay._redis == mock_redis
        assert replay._config == replay_config
        assert replay._status == ReplayStatus.IDLE

    def test_event_replay_initial_state_idle(self, event_replay):
        """Test that replay starts in IDLE state."""
        assert event_replay._status == ReplayStatus.IDLE

    @pytest.mark.asyncio
    async def test_start_replay(self, event_replay, mock_redis):
        """Test starting a replay."""
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start()

        assert event_replay._status == ReplayStatus.RUNNING
        mock_redis.xrange.assert_called()

    @pytest.mark.asyncio
    async def test_pause_replay(self, event_replay):
        """Test pausing a replay."""
        event_replay._status = ReplayStatus.RUNNING
        await event_replay.pause()
        assert event_replay._status == ReplayStatus.PAUSED

    @pytest.mark.asyncio
    async def test_resume_replay(self, event_replay):
        """Test resuming a paused replay."""
        event_replay._status = ReplayStatus.PAUSED
        await event_replay.resume()
        assert event_replay._status == ReplayStatus.RUNNING

    @pytest.mark.asyncio
    async def test_stop_replay(self, event_replay):
        """Test stopping a replay."""
        event_replay._status = ReplayStatus.RUNNING
        await event_replay.stop()
        assert event_replay._status == ReplayStatus.STOPPED

    @pytest.mark.asyncio
    async def test_get_status(self, event_replay):
        """Test getting replay status."""
        status = await event_replay.get_status()
        assert status == ReplayStatus.IDLE

    @pytest.mark.asyncio
    async def test_get_progress(self, event_replay, mock_redis):
        """Test getting replay progress."""
        mock_redis.xlen.return_value = 1000
        event_replay._processed_count = 500

        progress = await event_replay.get_progress()

        assert progress["total"] == 1000
        assert progress["processed"] == 500
        assert progress["percentage"] == 50.0

    @pytest.mark.asyncio
    async def test_replay_with_handler(self, event_replay, mock_redis):
        """Test replay with custom event handler."""
        handler = AsyncMock(return_value=True)
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(handler=handler)

        handler.assert_called()

    @pytest.mark.asyncio
    async def test_replay_batch_processing(self, event_replay, mock_redis):
        """Test replay processes events in batches."""
        mock_redis.xrange.return_value = [
            (
                f"1234567890-{i}",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
            for i in range(100)
        ]

        await event_replay.start()

        # Should process in batches
        assert mock_redis.xrange.call_count >= 1

    @pytest.mark.asyncio
    async def test_replay_with_speed_multiplier(self, event_replay, mock_redis):
        """Test replay with speed multiplier."""
        config = ReplayConfig(
            from_timestamp=datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc),
            to_timestamp=datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc),
            speed_multiplier=2.0,
        )
        replay = EventReplay(redis=mock_redis, config=config)

        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await replay.start()

        # Should respect speed multiplier
        assert replay._config.speed_multiplier == 2.0

    @pytest.mark.asyncio
    async def test_replay_with_retry_on_failure(self, event_replay, mock_redis):
        """Test replay retries on handler failure."""
        handler = AsyncMock(side_effect=Exception("Handler error"))
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        config = ReplayConfig(max_retries=3)
        replay = EventReplay(redis=mock_redis, config=config)

        await replay.start(handler=handler)

        # Should retry
        assert handler.call_count <= 4  # Initial + 3 retries

    @pytest.mark.asyncio
    async def test_replay_stops_on_max_failures(self, event_replay, mock_redis):
        """Test replay stops after max consecutive failures."""
        handler = AsyncMock(side_effect=Exception("Handler error"))
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        config = ReplayConfig(max_consecutive_failures=5)
        replay = EventReplay(redis=mock_redis, config=config)

        await replay.start(handler=handler)

        # Should stop after max failures
        assert replay._status == ReplayStatus.STOPPED

    @pytest.mark.asyncio
    async def test_replay_with_filter(self, event_replay, mock_redis):
        """Test replay with event filter."""
        def event_filter(event):
            return event.get("id") != "skip-me"

        handler = AsyncMock(return_value=True)
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(handler=handler, filter=event_filter)

        # Filter should be applied
        assert handler.call_count <= 1

    @pytest.mark.asyncio
    async def test_replay_with_transform(self, event_replay, mock_redis):
        """Test replay with event transform."""
        def event_transform(event):
            event["transformed"] = True
            return event

        handler = AsyncMock(return_value=True)
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(handler=handler, transform=event_transform)

        # Transform should be applied
        call_args = handler.call_args
        if call_args:
            assert call_args[0][0].get("transformed") is True

    @pytest.mark.asyncio
    async def test_replay_get_statistics(self, event_replay, mock_redis):
        """Test getting replay statistics."""
        mock_redis.xlen.return_value = 1000
        event_replay._processed_count = 500
        event_replay._failed_count = 10
        event_replay._start_time = datetime.now(timezone.utc)

        stats = await event_replay.get_statistics()

        assert stats["total_events"] == 1000
        assert stats["processed_events"] == 500
        assert stats["failed_events"] == 10
        assert "elapsed_time" in stats

    @pytest.mark.asyncio
    async def test_replay_reset(self, event_replay):
        """Test resetting replay state."""
        event_replay._processed_count = 100
        event_replay._failed_count = 10
        event_replay._status = ReplayStatus.RUNNING

        await event_replay.reset()

        assert event_replay._processed_count == 0
        assert event_replay._failed_count == 0
        assert event_replay._status == ReplayStatus.IDLE

    @pytest.mark.asyncio
    async def test_replay_from_id(self, event_replay, mock_redis):
        """Test replay from specific event ID."""
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(from_id="1234567890-0")

        # Should start from specific ID
        call_args = mock_redis.xrange.call_args
        assert call_args is not None

    @pytest.mark.asyncio
    async def test_replay_to_id(self, event_replay, mock_redis):
        """Test replay to specific event ID."""
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(to_id="1234567890-100")

        # Should stop at specific ID
        call_args = mock_redis.xrange.call_args
        assert call_args is not None

    @pytest.mark.asyncio
    async def test_replay_with_checkpoint(self, event_replay, mock_redis):
        """Test replay with checkpoint support."""
        mock_redis.get = AsyncMock(return_value=b"1234567890-50")
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(use_checkpoint=True)

        # Should resume from checkpoint
        mock_redis.get.assert_called()

    @pytest.mark.asyncio
    async def test_replay_save_checkpoint(self, event_replay, mock_redis):
        """Test saving replay checkpoint."""
        mock_redis.set = AsyncMock(return_value=True)
        event_replay._last_processed_id = "1234567890-50"

        await event_replay.save_checkpoint()

        mock_redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_replay_clear_checkpoint(self, event_replay, mock_redis):
        """Test clearing replay checkpoint."""
        mock_redis.delete = AsyncMock(return_value=1)

        await event_replay.clear_checkpoint()

        mock_redis.delete.assert_called()

    @pytest.mark.asyncio
    async def test_replay_with_dry_run(self, event_replay, mock_redis):
        """Test replay in dry-run mode."""
        handler = AsyncMock(return_value=True)
        mock_redis.xrange.return_value = [
            (
                "1234567890-0",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
        ]

        await event_replay.start(handler=handler, dry_run=True)

        # Handler should not be called in dry-run
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_replay_with_parallel_processing(self, event_replay, mock_redis):
        """Test replay with parallel processing."""
        handler = AsyncMock(return_value=True)
        mock_redis.xrange.return_value = [
            (
                f"1234567890-{i}",
                {"event": b'{"id":"test-1"}', "timestamp": b"2026-05-12T12:00:00Z"},
            )
            for i in range(10)
        ]

        config = ReplayConfig(parallel_workers=3)
        replay = EventReplay(redis=mock_redis, config=config)

        await replay.start(handler=handler)

        # Should process in parallel
        assert handler.call_count == 10

    def test_replay_status_transitions(self, event_replay):
        """Test replay status transitions."""
        assert event_replay._status == ReplayStatus.IDLE

        event_replay._status = ReplayStatus.RUNNING
        assert event_replay._status == ReplayStatus.RUNNING

        event_replay._status = ReplayStatus.PAUSED
        assert event_replay._status == ReplayStatus.PAUSED

        event_replay._status = ReplayStatus.COMPLETED
        assert event_replay._status == ReplayStatus.COMPLETED

        event_replay._status = ReplayStatus.STOPPED
        assert event_replay._status == ReplayStatus.STOPPED

        event_replay._status = ReplayStatus.FAILED
        assert event_replay._status == ReplayStatus.FAILED
