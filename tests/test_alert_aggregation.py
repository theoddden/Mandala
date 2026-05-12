"""Test alert aggregation functionality."""

from __future__ import annotations

import pytest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

from mandala.core.events.envelope import MandalaEvent, new_event


def test_alert_init():
    """Test Alert initialization."""
    from mandala.core.alert_aggregation import Alert

    alert = Alert(
        id="test-id",
        type="test.alert",
        severity="high",
        message="Test alert message",
        source="test-source",
    )

    assert alert.id == "test-id"
    assert alert.type == "test.alert"
    assert alert.severity == "high"
    assert alert.message == "Test alert message"
    assert alert.source == "test-source"
    assert alert.timestamp is not None


def test_alert_to_dict():
    """Test Alert to_dict method."""
    from mandala.core.alert_aggregation import Alert

    alert = Alert(
        id="test-id",
        type="test.alert",
        severity="high",
        message="Test alert message",
        source="test-source",
    )

    data = alert.to_dict()

    assert data["id"] == "test-id"
    assert data["type"] == "test.alert"
    assert data["severity"] == "high"
    assert data["message"] == "Test alert message"
    assert data["source"] == "test-source"
    assert "timestamp" in data


def test_alert_from_dict():
    """Test Alert from_dict class method."""
    from mandala.core.alert_aggregation import Alert

    data = {
        "id": "test-id",
        "type": "test.alert",
        "severity": "high",
        "message": "Test alert message",
        "source": "test-source",
        "timestamp": "2024-01-01T00:00:00+00:00",
        "metadata": {"key": "value"},
    }

    alert = Alert.from_dict(data)

    assert alert.id == "test-id"
    assert alert.type == "test.alert"
    assert alert.severity == "high"
    assert alert.message == "Test alert message"
    assert alert.source == "test-source"
    assert alert.metadata == {"key": "value"}


def test_alert_group_init():
    """Test AlertGroup initialization."""
    from mandala.core.alert_aggregation import AlertGroup

    group = AlertGroup(
        id="test-group",
        alert_type="test.alert",
        source="test-source",
        severity="high",
    )

    assert group.id == "test-group"
    assert group.alert_type == "test.alert"
    assert group.source == "test-source"
    assert group.severity == "high"
    assert len(group.alerts) == 0
    assert group.created_at is not None


def test_alert_group_add_alert():
    """Test AlertGroup add_alert method."""
    from mandala.core.alert_aggregation import Alert, AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert")
    alert = Alert(id="test-id", type="test.alert", severity="high", message="Test")

    group.add_alert(alert)

    assert len(group.alerts) == 1
    assert group.alerts[0] == alert


def test_alert_group_get_count():
    """Test AlertGroup get_count method."""
    from mandala.core.alert_aggregation import Alert, AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert")
    alert1 = Alert(id="test-id-1", type="test.alert", severity="high", message="Test")
    alert2 = Alert(id="test-id-2", type="test.alert", severity="high", message="Test")

    group.add_alert(alert1)
    group.add_alert(alert2)

    assert group.get_count() == 2


def test_alert_group_to_dict():
    """Test AlertGroup to_dict method."""
    from mandala.core.alert_aggregation import Alert, AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert", severity="high")
    alert = Alert(id="test-id", type="test.alert", severity="high", message="Test")
    group.add_alert(alert)

    data = group.to_dict()

    assert data["id"] == "test-group"
    assert data["alert_type"] == "test.alert"
    assert data["severity"] == "high"
    assert data["count"] == 1
    assert len(data["alerts"]) == 1


def test_alert_group_is_empty():
    """Test AlertGroup is_empty method."""
    from mandala.core.alert_aggregation import AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert")

    assert group.is_empty() is True


def test_alert_group_clear():
    """Test AlertGroup clear method."""
    from mandala.core.alert_aggregation import Alert, AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert")
    alert = Alert(id="test-id", type="test.alert", severity="high", message="Test")
    group.add_alert(alert)

    group.clear()

    assert len(group.alerts) == 0


def test_alert_group_get_severity_distribution():
    """Test AlertGroup get_severity_distribution method."""
    from mandala.core.alert_aggregation import Alert, AlertGroup

    group = AlertGroup(id="test-group", alert_type="test.alert")
    alert1 = Alert(id="test-id-1", type="test.alert", severity="high", message="Test")
    alert2 = Alert(id="test-id-2", type="test.alert", severity="high", message="Test")
    alert3 = Alert(id="test-id-3", type="test.alert", severity="low", message="Test")

    group.add_alert(alert1)
    group.add_alert(alert2)
    group.add_alert(alert3)

    distribution = group.get_severity_distribution()

    assert distribution["high"] == 2
    assert distribution["low"] == 1


@pytest.mark.asyncio
async def test_alert_aggregator_init():
    """Test AlertAggregator initialization."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    aggregator = AlertAggregator(mock_redis, ttl=3600)

    assert aggregator._redis == mock_redis
    assert aggregator._aggregation_key_prefix == "mandala:alert:aggregation"
    assert aggregator._ttl == 3600


@pytest.mark.asyncio
async def test_aggregation_key():
    """Test _aggregation_key method."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    aggregator = AlertAggregator(mock_redis)

    event = new_event(
        type="test.alert",
        source="test",
        subject="test-entity",
        data={"severity": "high"},
    )

    key = aggregator._aggregation_key(event)

    assert key == "mandala:alert:aggregation:test.alert:test-entity:high"


@pytest.mark.asyncio
async def test_should_route_disabled():
    """Test should_route with aggregation disabled."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_aggregation_enabled = False
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity")

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator.should_route(event)

    assert result is True


@pytest.mark.asyncio
async def test_should_route_new_window():
    """Test should_route with new aggregation window."""
    from mandala.core.alert_aggregation import AlertAggregator

    import json

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_aggregation_enabled = True
    mock_settings.alert_suppression_enabled = False
    mock_settings.alert_aggregation_window_seconds = 300
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity", data={"severity": "high"})

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator.should_route(event)

    assert result is True
    mock_redis.setex.assert_called_once()


@pytest.mark.asyncio
async def test_should_route_existing_window():
    """Test should_route with existing aggregation window."""
    from mandala.core.alert_aggregation import AlertAggregator

    import json

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=json.dumps({"count": 1, "alert_ids": ["old-id"]}))
    mock_redis.setex = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_aggregation_enabled = True
    mock_settings.alert_suppression_enabled = False
    mock_settings.alert_aggregation_window_seconds = 300
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity", data={"severity": "high"})

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator.should_route(event)

    assert result is False
    mock_redis.setex.assert_called_once()


@pytest.mark.asyncio
async def test_is_suppressed_no_windows():
    """Test _is_suppressed with no suppression windows."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_suppression_windows = []
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity")

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator._is_suppressed(event)

    assert result is False


@pytest.mark.asyncio
async def test_is_suppressed_active_window():
    """Test _is_suppressed with active suppression window."""
    from mandala.core.alert_aggregation import AlertAggregator

    from datetime import UTC, datetime, timedelta

    mock_redis = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_suppression_windows = [
        {
            "start": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
            "end": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    ]
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity")

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator._is_suppressed(event)

    assert result is True


@pytest.mark.asyncio
async def test_is_suppressed_inactive_window():
    """Test _is_suppressed with inactive suppression window."""
    from mandala.core.alert_aggregation import AlertAggregator

    from datetime import UTC, datetime, timedelta

    mock_redis = AsyncMock()
    mock_settings = Mock()
    mock_settings.alert_suppression_windows = [
        {
            "start": (datetime.now(UTC) - timedelta(hours=2)).isoformat(),
            "end": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        }
    ]
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity")

    with patch("mandala.core.alert_aggregation.get_settings", return_value=mock_settings):
        result = await aggregator._is_suppressed(event)

    assert result is False


@pytest.mark.asyncio
async def test_get_aggregated_alerts():
    """Test get_aggregated_alerts method."""
    from mandala.core.alert_aggregation import AlertAggregator

    import json

    mock_redis = AsyncMock()
    mock_redis.scan = AsyncMock(return_value=(0, [b"mandala:alert:aggregation:test.alert:entity:high"]))
    mock_redis.get = AsyncMock(return_value=json.dumps({"count": 5, "alert_ids": ["id1", "id2"]}))
    aggregator = AlertAggregator(mock_redis)

    aggregated = await aggregator.get_aggregated_alerts()

    assert len(aggregated) == 1
    assert aggregated[0]["count"] == 5


@pytest.mark.asyncio
async def test_get_aggregated_alerts_empty():
    """Test get_aggregated_alerts with no alerts."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    mock_redis.scan = AsyncMock(return_value=(0, []))
    aggregator = AlertAggregator(mock_redis)

    aggregated = await aggregator.get_aggregated_alerts()

    assert len(aggregated) == 0


@pytest.mark.asyncio
async def test_flush_aggregation():
    """Test flush_aggregation method."""
    from mandala.core.alert_aggregation import AlertAggregator

    import json

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=json.dumps({"count": 5, "alert_ids": ["id1", "id2"]}))
    mock_redis.delete = AsyncMock()
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity", data={"severity": "high"})

    result = await aggregator.flush_aggregation(event)

    assert result is not None
    assert result["count"] == 5
    mock_redis.delete.assert_called_once()


@pytest.mark.asyncio
async def test_flush_aggregation_not_found():
    """Test flush_aggregation with no aggregation data."""
    from mandala.core.alert_aggregation import AlertAggregator

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    aggregator = AlertAggregator(mock_redis)

    event = new_event(type="test.alert", source="test", subject="test-entity")

    result = await aggregator.flush_aggregation(event)

    assert result is None
