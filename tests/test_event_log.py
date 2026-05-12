"""Test event log functionality."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, Mock, patch

from mandala.core.events.envelope import MandalaEvent, new_event


def test_event_log_protocol():
    """Test EventLog protocol definition."""
    from mandala.core.event_log import EventLog

    # EventLog is a Protocol, so we can't instantiate it directly
    # Just verify it has the required method signatures
    assert hasattr(EventLog, "append")
    assert hasattr(EventLog, "query")


def test_pyiceberg_availability():
    """Test pyiceberg availability check."""
    from mandala.core.event_log import PYICEBERG_AVAILABLE, Catalog, Table

    # Just check the module loads correctly
    assert PYICEBERG_AVAILABLE in (True, False)


@pytest.mark.asyncio
async def test_iceberg_event_log_init():
    """Test IcebergEventLog initialization."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog

    mock_catalog = Mock()
    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    assert log._catalog == mock_catalog
    assert log._table_name == "test.events"
    assert log._namespace == "test"
    assert log._table is None
    assert log._initialized is False


@pytest.mark.asyncio
async def test_iceberg_event_log_ensure_table():
    """Test IcebergEventLog _ensure_table method."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog

    mock_catalog = Mock()
    mock_table = Mock()
    mock_snapshot = Mock()
    mock_snapshot.snapshot_id = "test-snapshot-id"
    mock_table.current_snapshot = Mock(return_value=mock_snapshot)
    mock_catalog.create_namespace = Mock()
    mock_catalog.create_table = Mock(return_value=mock_table)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    table = await log._ensure_table()

    assert table == mock_table
    assert log._initialized is True
    mock_catalog.create_namespace.assert_called_once()
    mock_catalog.create_table.assert_called_once()


@pytest.mark.asyncio
async def test_iceberg_event_log_ensure_table_existing():
    """Test IcebergEventLog _ensure_table with existing table."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog

    mock_catalog = Mock()
    mock_table = Mock()
    mock_table.current_snapshot = Mock(return_value=Mock(snapshot_id="test-snapshot-id"))
    mock_catalog.create_namespace = Mock(side_effect=Exception("Namespace exists"))
    mock_catalog.create_table = Mock(side_effect=Exception("Table exists"))
    mock_catalog.load_table = Mock(return_value=mock_table)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    table = await log._ensure_table()

    assert table == mock_table
    mock_catalog.load_table.assert_called_once()


@pytest.mark.asyncio
async def test_iceberg_event_log_append():
    """Test IcebergEventLog append method."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog

    mock_catalog = Mock()
    mock_table = Mock()
    mock_snapshot = Mock()
    mock_snapshot.snapshot_id = "test-snapshot-id"
    mock_table.current_snapshot = Mock(return_value=mock_snapshot)
    mock_catalog.create_namespace = Mock()
    mock_catalog.create_table = Mock(return_value=mock_table)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    event = new_event(type="test.event", source="test", subject="test-entity")
    snapshot_id = await log.append(event)

    assert snapshot_id == "test-snapshot-id"
    mock_table.append.assert_called_once()


@pytest.mark.asyncio
async def test_iceberg_event_log_query():
    """Test IcebergEventLog query method."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog

    mock_catalog = Mock()
    mock_table = Mock()
    mock_table.current_snapshot = Mock(return_value=Mock(snapshot_id="test-snapshot-id"))
    mock_catalog.create_namespace = Mock()
    mock_catalog.create_table = Mock(return_value=mock_table)

    # Mock PyArrow table
    mock_arrow_table = Mock()
    mock_arrow_table.num_rows = 1
    mock_arrow_table.slice = Mock(return_value=Mock(to_pydict=Mock(return_value={
        "id": ["test-id"],
        "source": ["test"],
        "type": ["test.event"],
        "specversion": ["1.0"],
        "time": ["2024-01-01T00:00:00Z"],
        "subject": ["test-entity"],
        "datacontenttype": ["application/json"],
        "data": ['{"test": "value"}'],
        "mandalaschemaversion": ["1.0"],
        "mandalaingestid": [None],
        "mandalaidempotencykey": [None],
        "traceparent": [None],
        "tracestate": [None],
        "received_at": [None],
        "processed_at": [None],
        "trace_id": [None],
        "span_id": [None],
        "parent_span_id": [None],
        "end_time": [None],
        "attributes": [None],
    })))
    mock_scan_result = Mock()
    mock_scan_result.to_arrow = Mock(return_value=mock_arrow_table)
    mock_table.scan = Mock(return_value=mock_scan_result)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    events = []
    async for event in log.query(subject="test-entity"):
        events.append(event)

    assert len(events) == 1
    assert events[0].type == "test.event"


@pytest.mark.asyncio
async def test_iceberg_event_log_query_with_time_range():
    """Test IcebergEventLog query with time range filter."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog
    from datetime import UTC, datetime

    mock_catalog = Mock()
    mock_table = Mock()
    mock_table.current_snapshot = Mock(return_value=Mock(snapshot_id="test-snapshot-id"))
    mock_catalog.create_namespace = Mock()
    mock_catalog.create_table = Mock(return_value=mock_table)

    # Mock PyArrow table
    mock_arrow_table = Mock()
    mock_arrow_table.num_rows = 0
    mock_arrow_table.slice = Mock(return_value=Mock(to_pydict=Mock(return_value={})))
    mock_scan_result = Mock()
    mock_scan_result.to_arrow = Mock(return_value=mock_arrow_table)
    mock_table.scan = Mock(return_value=mock_scan_result)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    events = []
    async for event in log.query(
        time_range=(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC))
    ):
        events.append(event)

    assert len(events) == 0


@pytest.mark.asyncio
async def test_iceberg_event_log_query_time_travel():
    """Test IcebergEventLog query with time travel."""
    pytest.importorskip("pyiceberg")

    from mandala.core.event_log import IcebergEventLog
    from datetime import UTC, datetime

    mock_catalog = Mock()
    mock_table = Mock()
    mock_snapshot = Mock()
    mock_snapshot.snapshot_id = "old-snapshot-id"
    mock_snapshot.timestamp_ms = 1000000
    mock_table.current_snapshot = Mock(return_value=Mock(snapshot_id="current-snapshot-id"))
    mock_table.snapshots = Mock(return_value=[mock_snapshot])
    mock_catalog.create_namespace = Mock()
    mock_catalog.create_table = Mock(return_value=mock_table)

    # Mock PyArrow table
    mock_arrow_table = Mock()
    mock_arrow_table.num_rows = 0
    mock_arrow_table.slice = Mock(return_value=Mock(to_pydict=Mock(return_value={})))
    mock_scan_result = Mock()
    mock_scan_result.to_arrow = Mock(return_value=mock_arrow_table)
    mock_table.scan = Mock(return_value=mock_scan_result)

    log = IcebergEventLog(mock_catalog, table_name="test.events", namespace="test")

    events = []
    async for event in log.query(as_of=datetime(2024, 1, 1, tzinfo=UTC)):
        events.append(event)

    assert len(events) == 0


def test_get_event_log():
    """Test get_event_log function."""
    from mandala.core.event_log import get_event_log, set_event_log

    # Initially None
    assert get_event_log() is None

    # Set and get
    mock_log = Mock()
    set_event_log(mock_log)
    assert get_event_log() == mock_log


def test_set_event_log():
    """Test set_event_log function."""
    from mandala.core.event_log import set_event_log

    mock_log = Mock()
    set_event_log(mock_log)
    # Just verify it doesn't raise
