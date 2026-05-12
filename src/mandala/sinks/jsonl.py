"""Newline-delimited JSON sink — files rotated daily.

Layout:
    <root>/dt=YYYY-MM-DD/raw_mandala_events.jsonl

This is exactly the layout Snowflake / BigQuery / Redshift / Athena
expect for date-partitioned external tables.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path

import structlog

from mandala.sinks.base import SinkRecord

log = structlog.get_logger(__name__)


class JsonlFileSink:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._current_date: date | None = None
        self._current_path: Path | None = None

    def _path_for(self, dt: datetime) -> Path:
        d = dt.date()
        if d != self._current_date:
            partition = self._root / f"dt={d.isoformat()}"
            partition.mkdir(parents=True, exist_ok=True)
            self._current_date = d
            self._current_path = partition / "raw_mandala_events.jsonl"
        assert self._current_path is not None
        return self._current_path

    async def write_batch(self, records: Iterable[SinkRecord]) -> None:
        records = list(records)
        if not records:
            return
        path = self._path_for(datetime.now(UTC))
        with path.open("a", encoding="utf-8") as f:
            for r in records:
                f.write(r.to_jsonl_line())
        log.info("sink.jsonl.write", count=len(records), path=str(path))

    async def aclose(self) -> None:
        return None
