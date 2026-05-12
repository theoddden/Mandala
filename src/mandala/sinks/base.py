"""Sink protocol and the canonical row shape consumed by ``dbt-mandala``."""
from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Protocol

from mandala.core.events.envelope import MandalaEvent


@dataclass(frozen=True, slots=True)
class SinkRecord:
    """Flat row matching the ``raw_mandala_events`` warehouse table."""

    event_id: str
    event_type: str
    source: str
    subject: str | None
    event_time: str            # RFC 3339
    ingested_at: str           # RFC 3339, set by the sink
    schema_version: str
    payload: str               # JSON string of the CloudEvents ``data``

    @classmethod
    def from_event(cls, event: MandalaEvent) -> SinkRecord:
        return cls(
            event_id=event.id,
            event_type=event.type,
            source=event.source,
            subject=event.subject,
            event_time=event.time.astimezone(UTC).isoformat(),
            ingested_at=datetime.now(UTC).isoformat(),
            schema_version=event.mandalaschemaversion,
            payload=json.dumps(event.data, default=str) if event.data is not None else "null",
        )

    def to_jsonl_line(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":")) + "\n"


class Sink(Protocol):
    """Anything that knows how to persist a batch of :class:`SinkRecord`."""

    async def write_batch(self, records: Iterable[SinkRecord]) -> None: ...
    async def aclose(self) -> None: ...
