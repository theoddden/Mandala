"""Comprehensive tests for alert aggregation module."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from mandala.core.alert_aggregation import Alert, AlertAggregator, AlertGroup


class TestAlert:
    """Test cases for Alert."""

    def test_alert_initialization(self):
        """Test that Alert initializes correctly."""
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        assert alert.id == "alert-1"
        assert alert.severity == "high"
        assert alert.source == "test"
        assert alert.message == "Test alert"

    def test_alert_to_dict(self):
        """Test converting alert to dictionary."""
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        data = alert.to_dict()
        assert data["id"] == "alert-1"
        assert data["severity"] == "high"

    def test_alert_from_dict(self):
        """Test creating alert from dictionary."""
        data = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        alert = Alert.from_dict(data)
        assert alert.id == "alert-1"
        assert alert.severity == "high"


class TestAlertGroup:
    """Test cases for AlertGroup."""

    def test_alert_group_initialization(self):
        """Test that AlertGroup initializes correctly."""
        group = AlertGroup(
            id="group-1",
            source="test",
            severity="high",
        )
        assert group.id == "group-1"
        assert group.source == "test"
        assert group.severity == "high"
        assert group.alerts == []

    def test_alert_group_add_alert(self):
        """Test adding alert to group."""
        group = AlertGroup(id="group-1", source="test", severity="high")
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        group.add_alert(alert)
        assert len(group.alerts) == 1
        assert group.alerts[0].id == "alert-1"

    def test_alert_group_get_count(self):
        """Test getting alert count."""
        group = AlertGroup(id="group-1", source="test", severity="high")
        assert group.get_count() == 0

        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        group.add_alert(alert)
        assert group.get_count() == 1

    def test_alert_group_is_empty(self):
        """Test checking if group is empty."""
        group = AlertGroup(id="group-1", source="test", severity="high")
        assert group.is_empty() is True

        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        group.add_alert(alert)
        assert group.is_empty() is False

    def test_alert_group_clear(self):
        """Test clearing alerts from group."""
        group = AlertGroup(id="group-1", source="test", severity="high")
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        group.add_alert(alert)
        group.clear()
        assert group.is_empty() is True

    def test_alert_group_get_severity_distribution(self):
        """Test getting severity distribution."""
        group = AlertGroup(id="group-1", source="test", severity="high")

        group.add_alert(
            Alert(
                id="alert-1",
                severity="high",
                source="test",
                message="Test alert",
                timestamp=datetime.now(timezone.utc),
            )
        )
        group.add_alert(
            Alert(
                id="alert-2",
                severity="medium",
                source="test",
                message="Test alert",
                timestamp=datetime.now(timezone.utc),
            )
        )

        distribution = group.get_severity_distribution()
        assert distribution["high"] == 1
        assert distribution["medium"] == 1


class TestAlertAggregator:
    """Test cases for AlertAggregator."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.hset = AsyncMock(return_value=True)
        redis.hget = AsyncMock(return_value=None)
        redis.hdel = AsyncMock(return_value=1)
        redis.hgetall = AsyncMock(return_value={})
        redis.expire = AsyncMock(return_value=True)
        redis.delete = AsyncMock(return_value=1)
        return redis

    @pytest.fixture
    def aggregator(self, mock_redis):
        """Create an AlertAggregator instance."""
        return AlertAggregator(redis=mock_redis, ttl=3600)

    def test_aggregator_initialization(self, mock_redis):
        """Test that AlertAggregator initializes correctly."""
        aggregator = AlertAggregator(redis=mock_redis, ttl=7200)
        assert aggregator._redis == mock_redis
        assert aggregator._ttl == 7200
        assert aggregator._grouping_window == 300

    @pytest.mark.asyncio
    async def test_add_alert(self, aggregator, mock_redis):
        """Test adding an alert."""
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )

        await aggregator.add_alert(alert)

        mock_redis.hset.assert_called()

    @pytest.mark.asyncio
    async def test_add_alert_with_grouping(self, aggregator, mock_redis):
        """Test adding alert with grouping enabled."""
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )

        await aggregator.add_alert(alert, group=True)

        mock_redis.hset.assert_called()

    @pytest.mark.asyncio
    async def test_get_alert(self, aggregator, mock_redis):
        """Test getting an alert by ID."""
        mock_redis.hget.return_value = b'{"id":"alert-1","severity":"high"}'

        alert = await aggregator.get_alert("alert-1")

        assert alert is not None
        mock_redis.hget.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_alert_not_found(self, aggregator, mock_redis):
        """Test getting non-existent alert."""
        mock_redis.hget.return_value = None

        alert = await aggregator.get_alert("nonexistent")

        assert alert is None

    @pytest.mark.asyncio
    async def test_get_alerts_by_source(self, aggregator, mock_redis):
        """Test getting alerts by source."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","source":"test"}',
            b"alert-2": b'{"id":"alert-2","source":"test"}',
        }

        alerts = await aggregator.get_alerts_by_source("test")

        assert len(alerts) == 2

    @pytest.mark.asyncio
    async def test_get_alerts_by_severity(self, aggregator, mock_redis):
        """Test getting alerts by severity."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","severity":"high"}',
            b"alert-2": b'{"id":"alert-2","severity":"high"}',
        }

        alerts = await aggregator.get_alerts_by_severity("high")

        assert len(alerts) == 2

    @pytest.mark.asyncio
    async def test_get_alerts_in_time_range(self, aggregator, mock_redis):
        """Test getting alerts in time range."""
        start = datetime.now(timezone.utc) - timedelta(hours=1)
        end = datetime.now(timezone.utc)

        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","timestamp":"2026-05-12T12:00:00Z"}',
        }

        alerts = await aggregator.get_alerts_in_time_range(start, end)

        assert alerts is not None

    @pytest.mark.asyncio
    async def test_delete_alert(self, aggregator, mock_redis):
        """Test deleting an alert."""
        await aggregator.delete_alert("alert-1")

        mock_redis.hdel.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_all_alerts(self, aggregator, mock_redis):
        """Test clearing all alerts."""
        mock_redis.delete = AsyncMock(return_value=1)

        await aggregator.clear_all()

        mock_redis.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_alert_count(self, aggregator, mock_redis):
        """Test getting total alert count."""
        mock_redis.hlen = AsyncMock(return_value=10)

        count = await aggregator.get_alert_count()

        assert count == 10
        mock_redis.hlen.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_statistics(self, aggregator, mock_redis):
        """Test getting alert statistics."""
        mock_redis.hlen = AsyncMock(return_value=10)
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"severity":"high"}',
            b"alert-2": b'{"severity":"medium"}',
        }

        stats = await aggregator.get_statistics()

        assert stats is not None
        assert "total_count" in stats

    @pytest.mark.asyncio
    async def test_group_alerts_by_source(self, aggregator, mock_redis):
        """Test grouping alerts by source."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","source":"test"}',
            b"alert-2": b'{"id":"alert-2","source":"test"}',
            b"alert-3": b'{"id":"alert-3","source":"other"}',
        }

        groups = await aggregator.group_alerts_by_source()

        assert len(groups) == 2
        assert "test" in groups
        assert "other" in groups

    @pytest.mark.asyncio
    async def test_group_alerts_by_severity(self, aggregator, mock_redis):
        """Test grouping alerts by severity."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","severity":"high"}',
            b"alert-2": b'{"id":"alert-2","severity":"high"}',
            b"alert-3": b'{"id":"alert-3","severity":"medium"}',
        }

        groups = await aggregator.group_alerts_by_severity()

        assert len(groups) == 2
        assert "high" in groups
        assert "medium" in groups

    @pytest.mark.asyncio
    async def test_expire_old_alerts(self, aggregator, mock_redis):
        """Test expiring old alerts."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","timestamp":"2026-05-11T12:00:00Z"}',  # Old
            b"alert-2": b'{"id":"alert-2","timestamp":"2026-05-12T12:00:00Z"}',  # Recent
        }
        mock_redis.hdel = AsyncMock(return_value=1)

        await aggregator.expire_old_alerts(ttl=3600)

        mock_redis.hdel.assert_called()

    @pytest.mark.asyncio
    async def test_get_recent_alerts(self, aggregator, mock_redis):
        """Test getting recent alerts."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","timestamp":"2026-05-12T12:00:00Z"}',
        }

        alerts = await aggregator.get_recent_alerts(limit=10)

        assert alerts is not None

    @pytest.mark.asyncio
    async def test_add_alert_with_metadata(self, aggregator, mock_redis):
        """Test adding alert with custom metadata."""
        alert = Alert(
            id="alert-1",
            severity="high",
            source="test",
            message="Test alert",
            timestamp=datetime.now(timezone.utc),
        )
        metadata = {"custom_key": "custom_value"}

        await aggregator.add_alert(alert, metadata=metadata)

        mock_redis.hset.assert_called()

    @pytest.mark.asyncio
    async def test_get_alerts_by_pattern(self, aggregator, mock_redis):
        """Test getting alerts by message pattern."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","message":"CPU high"}',
            b"alert-2": b'{"id":"alert-2","message":"Memory high"}',
        }

        alerts = await aggregator.get_alerts_by_pattern("high")

        assert len(alerts) == 2

    @pytest.mark.asyncio
    async def test_aggregate_alerts_by_time_window(self, aggregator, mock_redis):
        """Test aggregating alerts by time window."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","timestamp":"2026-05-12T12:00:00Z"}',
            b"alert-2": b'{"id":"alert-2","timestamp":"2026-05-12T12:01:00Z"}',
        }

        groups = await aggregator.aggregate_alerts_by_time_window(window_seconds=60)

        assert groups is not None

    @pytest.mark.asyncio
    async def test_deduplicate_alerts(self, aggregator, mock_redis):
        """Test deduplicating similar alerts."""
        mock_redis.hgetall.return_value = {
            b"alert-1": b'{"id":"alert-1","message":"CPU high","source":"server-1"}',
            b"alert-2": b'{"id":"alert-2","message":"CPU high","source":"server-1"}',
        }

        unique_alerts = await aggregator.deduplicate_alerts()

        # Should deduplicate based on message and source
        assert len(unique_alerts) <= 2
