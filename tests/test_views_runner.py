"""Test views runner functionality."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest


@pytest.mark.asyncio
async def test_probe_redis_version():
    """Test _probe_redis_version function."""
    from mandala.views.runner import _probe_redis_version

    mock_redis = AsyncMock()
    mock_redis.info = AsyncMock(return_value={"redis_version": "7.0.0"})

    version = await _probe_redis_version(mock_redis)

    assert version == "7.0.0"
    mock_redis.info.assert_called_once_with("server")


@pytest.mark.asyncio
async def test_probe_redis_version_error():
    """Test _probe_redis_version with error."""
    from mandala.views.runner import _probe_redis_version

    mock_redis = AsyncMock()
    mock_redis.info = AsyncMock(side_effect=Exception("Redis error"))

    version = await _probe_redis_version(mock_redis)

    assert version == "unknown"


@pytest.mark.asyncio
async def test_publish_consumer_group_lag():
    """Test _publish_consumer_group_lag function."""
    from mandala.views.runner import _publish_consumer_group_lag

    mock_redis = AsyncMock()
    mock_redis.xinfo_groups = AsyncMock(return_value=[{"name": "test-group", "lag": 10}])

    # Run once then cancel
    task = asyncio.create_task(_publish_consumer_group_lag(mock_redis, "test-stream", "test-group", interval_sec=0.1))
    await asyncio.sleep(0.15)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    mock_redis.xinfo_groups.assert_called()


@pytest.mark.asyncio
async def test_publish_consumer_group_lag_no_group():
    """Test _publish_consumer_group_lag when group not found."""
    from mandala.views.runner import _publish_consumer_group_lag

    mock_redis = AsyncMock()
    mock_redis.xinfo_groups = AsyncMock(return_value=[{"name": "other-group", "lag": 10}])

    # Run once then cancel
    task = asyncio.create_task(_publish_consumer_group_lag(mock_redis, "test-stream", "test-group", interval_sec=0.1))
    await asyncio.sleep(0.15)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    mock_redis.xinfo_groups.assert_called()


def test_build_views():
    """Test _build_views function."""
    from mandala.views.runner import _build_views

    mock_redis = Mock()
    mock_settings = Mock()
    mock_settings.views_geospatial_enabled = True
    mock_settings.views_timeseries_enabled = True
    mock_settings.views_bitmap_enabled = False
    mock_settings.views_graph_enabled = False
    mock_settings.views_dead_zone_enabled = False

    with patch("mandala.views.runner.get_settings", return_value=mock_settings):
        views = _build_views(mock_redis)

    assert len(views) == 2
    assert views[0].name == "geospatial"
    assert views[1].name == "timeseries"


def test_build_views_all_enabled():
    """Test _build_views with all views enabled."""
    from mandala.views.runner import _build_views

    mock_redis = Mock()
    mock_settings = Mock()
    mock_settings.views_geospatial_enabled = True
    mock_settings.views_timeseries_enabled = True
    mock_settings.views_bitmap_enabled = True
    mock_settings.views_graph_enabled = True
    mock_settings.views_dead_zone_enabled = True

    with patch("mandala.views.runner.get_settings", return_value=mock_settings):
        views = _build_views(mock_redis)

    assert len(views) == 5


def test_build_views_none_enabled():
    """Test _build_views with no views enabled."""
    from mandala.views.runner import _build_views

    mock_redis = Mock()
    mock_settings = Mock()
    mock_settings.views_geospatial_enabled = False
    mock_settings.views_timeseries_enabled = False
    mock_settings.views_bitmap_enabled = False
    mock_settings.views_graph_enabled = False
    mock_settings.views_dead_zone_enabled = False

    with patch("mandala.views.runner.get_settings", return_value=mock_settings):
        views = _build_views(mock_redis)

    assert len(views) == 0


@pytest.mark.asyncio
async def test_rebuild_views():
    """Test _rebuild_views function."""
    from mandala.views.runner import _rebuild_views

    mock_redis = AsyncMock()
    mock_redis.keys = AsyncMock(return_value=["key1", "key2"])
    mock_redis.delete = AsyncMock()

    mock_view = Mock()
    mock_view.name = "test-view"

    await _rebuild_views(mock_redis, [mock_view])

    mock_redis.keys.assert_called()
    mock_redis.delete.assert_called()


@pytest.mark.asyncio
async def test_rebuild_views_error():
    """Test _rebuild_views with error."""
    from mandala.views.runner import _rebuild_views

    mock_redis = AsyncMock()
    mock_redis.keys = AsyncMock(side_effect=Exception("Redis error"))

    mock_view = Mock()
    mock_view.name = "test-view"

    # Should not raise
    await _rebuild_views(mock_redis, [mock_view])


@pytest.mark.asyncio
async def test_run_no_views():
    """Test run function with no views enabled."""
    from mandala.views.runner import run

    mock_redis = AsyncMock()
    mock_redis.aclose = AsyncMock()
    mock_redis.info = AsyncMock(return_value={"redis_version": "7.0.0"})

    mock_settings = Mock()
    mock_settings.redis_url = "redis://localhost"
    mock_settings.views_geospatial_enabled = False
    mock_settings.views_timeseries_enabled = False
    mock_settings.views_bitmap_enabled = False
    mock_settings.views_graph_enabled = False
    mock_settings.views_dead_zone_enabled = False

    with patch("mandala.views.runner.get_settings", return_value=mock_settings), patch(
        "mandala.views.runner.redis.from_url", return_value=mock_redis
    ):
        await run()

    mock_redis.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_run_with_views():
    """Test run function with views enabled."""
    from mandala.views.runner import run

    mock_redis = AsyncMock()
    mock_redis.aclose = AsyncMock()
    mock_redis.info = AsyncMock(return_value={"redis_version": "7.0.0"})

    mock_bus = AsyncMock()
    # Make consume raise CancelledError to break the loop cleanly
    mock_bus.consume = AsyncMock(side_effect=asyncio.CancelledError())

    mock_settings = Mock()
    mock_settings.redis_url = "redis://localhost"
    mock_settings.views_geospatial_enabled = True
    mock_settings.views_timeseries_enabled = False
    mock_settings.views_bitmap_enabled = False
    mock_settings.views_graph_enabled = False
    mock_settings.views_dead_zone_enabled = False
    mock_settings.stream_inbound = "mandala:inbound"
    mock_settings.views_consumer_group = "mandala:views"
    mock_settings.metrics_enabled = False

    with patch("mandala.views.runner.get_settings", return_value=mock_settings), patch(
        "mandala.views.runner.redis.from_url", return_value=mock_redis
    ), patch("mandala.views.runner.RedisStreamsBus", return_value=mock_bus), pytest.raises(
        asyncio.CancelledError
    ):
        await run()

    mock_bus.consume.assert_called()


def test_main():
    """Test main function."""
    from mandala.views.runner import main

    with patch("mandala.views.runner.asyncio.run") as mock_run:
        main()
        mock_run.assert_called_once()
