"""Tests for adaptive backpressure."""

from __future__ import annotations

import pytest


class MockRedis:
    """Mock Redis client for testing."""

    def __init__(self):
        self._latency_ms = 1.0
        self._memory_percent = 50.0
        self._cpu_percent = 30.0
        self._stream_length = 1000

    async def ping(self):
        """Mock ping."""
        return True

    async def info(self, section):
        """Mock info."""
        return {"redis_version": "7.0.0"}

    async def xlen(self, stream):
        """Mock xlen."""
        return self._stream_length


@pytest.mark.asyncio
async def test_adaptive_backpressure_healthy_system():
    """Test that healthy system accepts new events."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure

    mock_redis = MockRedis()
    backpressure = AdaptiveBackpressure(mock_redis)

    should_accept, reason = await backpressure.should_accept_new_event()

    assert should_accept is True
    assert "healthy" in reason.lower()


@pytest.mark.asyncio
async def test_adaptive_backpressure_high_memory():
    """Test that high memory usage rejects new events."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure

    mock_redis = MockRedis()
    mock_redis._memory_percent = 95.0  # Above default threshold of 90
    backpressure = AdaptiveBackpressure(mock_redis)

    should_accept, reason = await backpressure.should_accept_new_event()

    assert should_accept is False
    assert "degraded" in reason.lower()


@pytest.mark.asyncio
async def test_adaptive_backpressure_high_redis_latency():
    """Test that high Redis latency triggers batch size reduction."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure, PSUTIL_AVAILABLE

    if not PSUTIL_AVAILABLE:
        pytest.skip("psutil not available")

    mock_redis = MockRedis()
    mock_redis._latency_ms = 500.0  # Above default threshold of 100
    backpressure = AdaptiveBackpressure(mock_redis)

    health = await backpressure.check_health()
    batch_size = backpressure.adapt_batch_size(health)

    assert batch_size < backpressure._base_batch_size
    assert health["recommendation"] == "reduce_batch"


@pytest.mark.asyncio
async def test_adaptive_backpressure_health_history():
    """Test that health history is maintained."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure

    mock_redis = MockRedis()
    backpressure = AdaptiveBackpressure(mock_redis)

    # Check health multiple times
    await backpressure.check_health()
    await backpressure.check_health()
    await backpressure.check_health()

    history = backpressure.get_health_history()

    assert len(history) == 3


@pytest.mark.asyncio
async def test_adaptive_backpressure_high_memory():
    """Test that high memory usage triggers backpressure."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure, PSUTIL_AVAILABLE

    if not PSUTIL_AVAILABLE:
        pytest.skip("psutil not available")

    mock_redis = MockRedis()
    backpressure = AdaptiveBackpressure(mock_redis)

    # Simulate high memory usage
    health = {
        "is_healthy": False,
        "recommendation": "reject_new",
        "redis_latency_ms": 1.0,
        "memory_percent": 95.0,
        "cpu_percent": 30.0,
        "stream_length": 1000,
    }

    should_accept, reason = await backpressure.should_accept_new_event()
    assert should_accept is False
    assert "degraded" in reason.lower()


@pytest.mark.asyncio
async def test_adaptive_backpressure_batch_size_adjustment():
    """Test that batch size is adjusted based on health."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure

    mock_redis = MockRedis()
    backpressure = AdaptiveBackpressure(mock_redis)

    # Simulate unhealthy system
    health = {
        "is_healthy": False,
        "recommendation": "reduce_batch",
        "redis_latency_ms": 500.0,
        "memory_percent": 50.0,
        "cpu_percent": 30.0,
        "stream_length": 1000,
    }

    batch_size = backpressure.adapt_batch_size(health)

    assert batch_size < backpressure._base_batch_size
    assert batch_size >= backpressure._min_batch_size


@pytest.mark.asyncio
async def test_adaptive_backpressure_batch_size_recovery():
    """Test that batch size recovers when system is healthy."""
    from mandala.core.adaptive_backpressure import AdaptiveBackpressure

    mock_redis = MockRedis()
    backpressure = AdaptiveBackpressure(mock_redis)

    # Reduce batch size to a value that can actually increase
    backpressure._current_batch_size = 5

    # Simulate healthy system
    health = {
        "is_healthy": True,
        "recommendation": "normal",
        "redis_latency_ms": 1.0,
        "memory_percent": 50.0,
        "cpu_percent": 30.0,
        "stream_length": 1000,
    }

    batch_size = backpressure.adapt_batch_size(health)

    assert batch_size > 5
    assert batch_size <= backpressure._base_batch_size
