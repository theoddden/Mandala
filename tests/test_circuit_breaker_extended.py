"""Extended comprehensive tests for circuit breaker."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.circuit_breaker import CircuitBreaker, CircuitBreakerState


class TestCircuitBreakerExtended:
    """Extended test cases for CircuitBreaker."""

    @pytest.fixture
    def circuit_breaker(self):
        """Create a CircuitBreaker instance."""
        return CircuitBreaker(
            name="test_breaker",
            failure_threshold=5,
            recovery_timeout=60,
            half_open_max_calls=3,
        )

    def test_circuit_breaker_initialization_custom(self):
        """Test initialization with custom parameters."""
        cb = CircuitBreaker(
            name="custom_breaker",
            failure_threshold=10,
            recovery_timeout=120,
            half_open_max_calls=5,
            success_threshold=3,
        )
        assert cb._failure_threshold == 10
        assert cb._recovery_timeout == 120
        assert cb._half_open_max_calls == 5
        assert cb._success_threshold == 3

    def test_circuit_breaker_initial_state_closed(self, circuit_breaker):
        """Test that circuit breaker starts in closed state."""
        assert circuit_breaker.state == CircuitBreakerState.CLOSED

    def test_circuit_breaker_failure_count_initial_zero(self, circuit_breaker):
        """Test that failure count starts at zero."""
        assert circuit_breaker._failure_count == 0

    def test_circuit_breaker_success_count_initial_zero(self, circuit_breaker):
        """Test that success count starts at zero."""
        assert circuit_breaker._success_count == 0

    def test_circuit_breaker_last_failure_time_initial_none(self, circuit_breaker):
        """Test that last failure time starts as None."""
        assert circuit_breaker._last_failure_time is None

    @pytest.mark.asyncio
    async def test_circuit_breaker_records_success(self, circuit_breaker):
        """Test that successful calls are recorded."""

        async def success_func():
            return "success"

        result = await circuit_breaker.call(success_func)

        assert result == "success"
        assert circuit_breaker._success_count == 1
        assert circuit_breaker._failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_records_failure(self, circuit_breaker):
        """Test that failed calls are recorded."""

        async def fail_func():
            raise Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            await circuit_breaker.call(fail_func)

        assert circuit_breaker._failure_count == 1
        assert circuit_breaker._success_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_threshold(self, circuit_breaker):
        """Test that circuit opens after failure threshold is reached."""

        async def fail_func():
            raise Exception("Test error")

        # Trigger failures
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        assert circuit_breaker.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    async def test_circuit_breaker_rejects_when_open(self, circuit_breaker):
        """Test that calls are rejected when circuit is open."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        # Try to call when open
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            await circuit_breaker.call(fail_func)

    @pytest.mark.asyncio
    async def test_circuit_breaker_transitions_to_half_open_after_timeout(self, circuit_breaker):
        """Test that circuit transitions to half-open after timeout."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        # Set last failure time to past
        circuit_breaker._last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=61)

        # Check state
        await circuit_breaker.check_state()

        assert circuit_breaker.state == CircuitBreakerState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_circuit_breaker_closes_after_success_threshold(self, circuit_breaker):
        """Test that circuit closes after success threshold in half-open."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        # Set last failure time to past to transition to half-open
        circuit_breaker._last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=61)
        await circuit_breaker.check_state()

        # Reset success count
        circuit_breaker._success_count = 0

        # Make successful calls
        async def success_func():
            return "success"

        for _ in range(3):
            await circuit_breaker.call(success_func)

        assert circuit_breaker.state == CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_circuit_breaker_reopens_on_failure_in_half_open(self, circuit_breaker):
        """Test that circuit reopens if failure occurs in half-open state."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        # Transition to half-open
        circuit_breaker._last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=61)
        await circuit_breaker.check_state()

        # Fail in half-open
        with pytest.raises(Exception, match="Test error"):
            await circuit_breaker.call(fail_func)

        assert circuit_breaker.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    async def test_circuit_breaker_reset(self, circuit_breaker):
        """Test resetting the circuit breaker."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        assert circuit_breaker.state == CircuitBreakerState.OPEN

        # Reset
        circuit_breaker.reset()

        assert circuit_breaker.state == CircuitBreakerState.CLOSED
        assert circuit_breaker._failure_count == 0
        assert circuit_breaker._success_count == 0
        assert circuit_breaker._last_failure_time is None

    @pytest.mark.asyncio
    async def test_circuit_breaker_get_stats(self, circuit_breaker):
        """Test getting circuit breaker statistics."""

        async def fail_func():
            raise Exception("Test error")

        # Trigger some failures
        for _ in range(3):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        stats = circuit_breaker.get_stats()

        assert stats["state"] == CircuitBreakerState.CLOSED
        assert stats["failure_count"] == 3
        assert stats["success_count"] == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_context_manager_success(self, circuit_breaker):
        """Test using circuit breaker as context manager for success."""
        async with circuit_breaker:
            result = await asyncio.sleep(0.01, result="success")
            assert result == "success"

        assert circuit_breaker._success_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_context_manager_failure(self, circuit_breaker):
        """Test using circuit breaker as context manager for failure."""
        with pytest.raises(Exception, match="Test error"):
            async with circuit_breaker:
                raise Exception("Test error")

        assert circuit_breaker._failure_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_context_manager_rejects_when_open(self, circuit_breaker):
        """Test context manager rejects when circuit is open."""

        async def fail_func():
            raise Exception("Test error")

        # Open the circuit
        for _ in range(5):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        # Try to use context manager when open
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            async with circuit_breaker:
                pass

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_custom_exception_handler(self, circuit_breaker):
        """Test circuit breaker with custom exception handler."""
        handler_called = []

        def custom_handler(exc):
            handler_called.append(exc)
            return True  # Should count as failure

        circuit_breaker.set_exception_handler(custom_handler)

        async def fail_func():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            await circuit_breaker.call(fail_func)

        assert len(handler_called) == 1
        assert circuit_breaker._failure_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_exception_filter(self, circuit_breaker):
        """Test circuit breaker with exception filter."""
        # Only count ValueError as failure
        circuit_breaker.set_exception_filter(lambda exc: isinstance(exc, ValueError))

        async def value_error_func():
            raise ValueError("Test error")

        async def type_error_func():
            raise TypeError("Test error")

        # ValueError should count as failure
        with pytest.raises(ValueError):
            await circuit_breaker.call(value_error_func)
        assert circuit_breaker._failure_count == 1

        # TypeError should not count as failure
        with pytest.raises(TypeError):
            await circuit_breaker.call(type_error_func)
        assert circuit_breaker._failure_count == 1  # Still 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_timeout(self, circuit_breaker):
        """Test circuit breaker with call timeout."""

        async def slow_func():
            await asyncio.sleep(2)
            return "success"

        # Set short timeout
        with pytest.raises(asyncio.TimeoutError):
            await circuit_breaker.call(slow_func, timeout=0.1)

        # Timeout should count as failure
        assert circuit_breaker._failure_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_concurrent_calls(self, circuit_breaker):
        """Test circuit breaker with concurrent calls."""

        async def success_func():
            await asyncio.sleep(0.01)
            return "success"

        # Make concurrent calls
        tasks = [circuit_breaker.call(success_func) for _ in range(10)]
        results = await asyncio.gather(*tasks)

        assert all(r == "success" for r in results)
        assert circuit_breaker._success_count == 10

    @pytest.mark.asyncio
    async def test_circuit_breaker_fallback_function(self, circuit_breaker):
        """Test circuit breaker with fallback function."""

        async def fail_func():
            raise Exception("Test error")

        async def fallback_func():
            return "fallback"

        result = await circuit_breaker.call(fail_func, fallback=fallback_func)

        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_circuit_breaker_fallback_not_called_on_success(self, circuit_breaker):
        """Test that fallback is not called on success."""

        async def success_func():
            return "success"

        async def fallback_func():
            return "fallback"

        result = await circuit_breaker.call(success_func, fallback=fallback_func)

        assert result == "success"

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_retry_on_failure(self, circuit_breaker):
        """Test circuit breaker with retry logic."""
        call_count = [0]

        async def flaky_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception("Temporary error")
            return "success"

        result = await circuit_breaker.call(flaky_func, max_retries=3)

        assert result == "success"
        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_retry_exhausted(self, circuit_breaker):
        """Test circuit breaker when retries are exhausted."""

        async def fail_func():
            raise Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            await circuit_breaker.call(fail_func, max_retries=3)

        # Each retry counts as a failure
        assert circuit_breaker._failure_count == 4  # Initial + 3 retries

    @pytest.mark.asyncio
    async def test_circuit_breaker_metrics_collection(self, circuit_breaker):
        """Test that circuit breaker collects metrics."""

        async def success_func():
            return "success"

        async def fail_func():
            raise Exception("Test error")

        # Mix of successes and failures
        for _ in range(3):
            await circuit_breaker.call(success_func)

        for _ in range(2):
            with pytest.raises(Exception, match="Test error"):
                await circuit_breaker.call(fail_func)

        metrics = circuit_breaker.get_metrics()

        assert metrics["total_calls"] == 5
        assert metrics["successful_calls"] == 3
        assert metrics["failed_calls"] == 2
        assert metrics["success_rate"] == 0.6

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_sliding_window(self):
        """Test circuit breaker with sliding window for failure tracking."""
        cb = CircuitBreaker(name="sliding_window_breaker", failure_threshold=5, sliding_window_size=10)

        async def fail_func():
            raise Exception("Test error")

        async def success_func():
            return "success"

        # Mix of calls
        for i in range(10):
            if i % 2 == 0:
                with pytest.raises(Exception, match="Test error"):
                    await cb.call(fail_func)
            else:
                await cb.call(success_func)

        # Should not open because failures are within threshold
        assert cb.state == CircuitBreakerState.CLOSED

    def test_circuit_breaker_str_representation(self, circuit_breaker):
        """Test string representation of circuit breaker."""
        str_repr = str(circuit_breaker)
        assert "CircuitBreaker" in str_repr
        assert "CLOSED" in str_repr

    def test_circuit_breaker_repr(self, circuit_breaker):
        """Test repr of circuit breaker."""
        repr_str = repr(circuit_breaker)
        assert "CircuitBreaker" in repr_str
