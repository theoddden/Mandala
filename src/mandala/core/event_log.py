"""Event log abstraction for permanent event storage.

The default implementation uses Apache Iceberg on object storage (S3/GCS/Azure)
for permanent, immutable event storage. This separates the ephemeral event bus
(Redis Streams) from the permanent event log (Iceberg).

Iceberg provides:
- Infinite retention at object storage costs
- Time travel queries for audit/compliance
- Schema evolution without breaking queries
- Direct query from Snowflake, DuckDB, Spark, Trino, ClickHouse
- Partition pruning for efficient queries

The event log is append-only. Events are never deleted or modified.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, UTC
from typing import Any, Protocol

import structlog

from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)

# Optional: pyiceberg is only needed if event_log_enabled is set
try:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False
    Catalog = None  # type: ignore
    Table = None  # type: ignore


class EventLog(Protocol):
    """Permanent event log interface."""

    async def append(self, event: MandalaEvent) -> str:
        """Atomically append event to log; returns snapshot ID."""
        ...

    async def query(
        self,
        subject: str | None = None,
        event_type: str | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        as_of: datetime | None = None,  # time travel
    ) -> AsyncIterator[MandalaEvent]:
        """Query event log with filters."""
        ...


class IcebergEventLog:
    """Iceberg-backed event log on object storage."""

    def __init__(
        self,
        catalog: Catalog,
        table_name: str = "mandala.events",
        namespace: str = "mandala",
    ):
        self._catalog = catalog
        self._table_name = table_name
        self._namespace = namespace
        self._table: Table | None = None
        self._initialized = False

    async def _ensure_table(self) -> Table:
        """Ensure Iceberg table exists with proper schema."""
        if self._initialized and self._table is not None:
            return self._table

        # Run in executor since PyIceberg operations are synchronous
        loop = asyncio.get_event_loop()

        def _create_or_load():
            from pyiceberg.partitioning import PartitionSpec
            from pyiceberg.schema import Schema
            from pyiceberg.transforms import IdentityTransform
            from pyiceberg.types import NestedField, StringType, TimestampType

            # Define schema matching MandalaEvent
            schema = Schema(
                NestedField(1, "id", StringType(), required=True),
                NestedField(2, "source", StringType(), required=True),
                NestedField(3, "type", StringType(), required=True),
                NestedField(4, "specversion", StringType(), required=True),
                NestedField(5, "time", TimestampType(), required=True),
                NestedField(6, "subject", StringType()),
                NestedField(7, "datacontenttype", StringType()),
                NestedField(8, "data", StringType(), doc="JSON payload"),
                NestedField(9, "mandalaschemaversion", StringType()),
                NestedField(10, "mandalaingestid", StringType()),
                NestedField(11, "mandalaidempotencykey", StringType()),
                NestedField(12, "traceparent", StringType()),
                NestedField(13, "tracestate", StringType()),
                NestedField(14, "received_at", TimestampType()),
                NestedField(15, "processed_at", TimestampType()),
                NestedField(16, "trace_id", StringType()),
                NestedField(17, "span_id", StringType()),
                NestedField(18, "parent_span_id", StringType()),
                NestedField(19, "end_time", TimestampType()),
                NestedField(20, "attributes", StringType(), doc="JSON dict"),
            )

            # Ensure namespace exists
            try:
                self._catalog.create_namespace(self._namespace)
            except Exception:
                # Namespace already exists
                pass

            # Create or load table
            try:
                table = self._catalog.create_table(
                    f"{self._namespace}.{self._table_name}",
                    schema=schema,
                    partition_spec=PartitionSpec(
                        IdentityTransform("time"),  # Daily partitions
                        IdentityTransform("type"),  # Sub-partition by event type
                    ),
                    properties={
                        "write.format.default": "parquet",
                        "write.compression-codec": "zstd",
                        "write.target-file-size-bytes": str(256 * 1024 * 1024),  # 256MB
                    },
                )
                log.info("iceberg.table.created", table=f"{self._namespace}.{self._table_name}")
            except Exception:
                # Table already exists
                table = self._catalog.load_table(f"{self._namespace}.{self._table_name}")
                log.info("iceberg.table.loaded", table=f"{self._namespace}.{self._table_name}")

            return table

        self._table = await loop.run_in_executor(None, _create_or_load)
        self._initialized = True
        return self._table

    async def append(self, event: MandalaEvent) -> str:
        """Append event to Iceberg table via PyArrow."""
        table = await self._ensure_table()

        # Convert MandalaEvent to PyArrow table
        loop = asyncio.get_event_loop()

        def _append():
            import pyarrow as pa

            # Build PyArrow schema for single row
            data = {
                "id": [event.id],
                "source": [event.source],
                "type": [event.type],
                "specversion": [event.specversion],
                "time": [event.time],
                "subject": [event.subject],
                "datacontenttype": [event.datacontenttype],
                "data": [event.model_dump_json(exclude_none=True, by_alias=True)],
                "mandalaschemaversion": [event.mandalaschemaversion],
                "mandalaingestid": [event.mandalaingestid],
                "mandalaidempotencykey": [event.mandalaidempotencykey],
                "traceparent": [event.traceparent],
                "tracestate": [event.tracestate],
                "received_at": [event.received_at],
                "processed_at": [event.processed_at],
                "trace_id": [event.trace_id],
                "span_id": [event.span_id],
                "parent_span_id": [event.parent_span_id],
                "end_time": [event.end_time],
                "attributes": [event.model_dump_json(exclude_none=True, by_alias=True) if event.attributes else None],
            }

            arrow_table = pa.table(data)
            table.append(arrow_table)

            # Return snapshot ID
            snapshot_id = table.current_snapshot().snapshot_id
            return str(snapshot_id)

        snapshot_id = await loop.run_in_executor(None, _append)
        log.debug("iceberg.event.appended", event_id=event.id, snapshot_id=snapshot_id)
        return snapshot_id

    async def query(
        self,
        subject: str | None = None,
        event_type: str | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        as_of: datetime | None = None,
    ) -> AsyncIterator[MandalaEvent]:
        """Query event log with filters."""
        table = await self._ensure_table()
        loop = asyncio.get_event_loop()

        def _scan():
            from pyarrow.compute import field

            # Build row filter
            filters = []
            if subject:
                filters.append(field("subject") == subject)
            if event_type:
                filters.append(field("type") == event_type)
            if time_range:
                start, end = time_range
                filters.append((field("time") >= start) & (field("time") <= end))

            row_filter = filters[0] if filters else None
            if len(filters) > 1:
                from pyarrow.compute import and_

                for f in filters[1:]:
                    row_filter = and_(row_filter, f) if row_filter else f

            # Scan with filter
            if as_of:
                # Time travel: find snapshot at or before as_of
                snapshots = list(table.snapshots())
                target_snapshot = None
                for snap in reversed(snapshots):
                    if snap.timestamp_ms / 1000 <= as_of.timestamp():
                        target_snapshot = snap.snapshot_id
                        break
                if target_snapshot:
                    arrow_table = table.scan(row_filter=row_filter, snapshot_id=target_snapshot).to_arrow()
                else:
                    arrow_table = table.scan(row_filter=row_filter).to_arrow()
            else:
                arrow_table = table.scan(row_filter=row_filter).to_arrow()

            return arrow_table

        arrow_table = await loop.run_in_executor(None, _scan)

        # Convert rows back to MandalaEvent
        for i in range(arrow_table.num_rows):
            row = arrow_table.slice(i, 1).to_pydict()
            # Extract JSON fields
            data = row.get("data", [None])[0]
            if isinstance(data, str):
                import json

                data = json.loads(data)

            attributes = row.get("attributes", [None])[0]
            if isinstance(attributes, str):
                import json

                attributes = json.loads(attributes)

            event = MandalaEvent(
                id=row["id"][0],
                source=row["source"][0],
                type=row["type"][0],
                specversion=row["specversion"][0],
                time=row["time"][0],
                subject=row["subject"][0],
                datacontenttype=row["datacontenttype"][0],
                data=data,
                mandalaschemaversion=row["mandalaschemaversion"][0],
                mandalaingestid=row["mandalaingestid"][0],
                mandalaidempotencykey=row["mandalaidempotencykey"][0],
                traceparent=row["traceparent"][0],
                tracestate=row["tracestate"][0],
                received_at=row["received_at"][0],
                processed_at=row["processed_at"][0],
                trace_id=row["trace_id"][0],
                span_id=row["span_id"][0],
                parent_span_id=row["parent_span_id"][0],
                end_time=row["end_time"][0],
                attributes=attributes,
            )
            yield event


# Singleton instance
_event_log: EventLog | None = None


def get_event_log() -> EventLog | None:
    """Get the global event log instance (or None if not configured)."""
    return _event_log


def set_event_log(event_log: EventLog) -> None:
    """Set the global event log instance."""
    global _event_log
    _event_log = event_log
