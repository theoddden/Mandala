"""CloudEvents 1.0 envelope used as the bridge format.

This is the *only* shape that crosses an internal boundary in Mandala —
webhook ingest, the Redis Streams bus, projection workers, playbook
matchers, and the MCP layer all work with :class:`MandalaEvent`. Connectors
live at the edges and convert their vendor formats to/from this envelope.

The envelope conforms to `CloudEvents 1.0
<https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md>`_:

* ``id`` — globally unique event id (UUIDv7 by default for time-ordering).
* ``source`` — URI identifier for the producer (e.g. ``mandala/connector/samsara``).
* ``type`` — value from :class:`mandala.core.events.types.EventType`.
* ``specversion`` — fixed at ``"1.0"``.
* ``time`` — RFC 3339 timestamp of when the producer observed the fact.
* ``subject`` — Mandala URN of the entity (truck, shipment, party, ...).
* ``datacontenttype`` — defaults to ``application/json``.
* ``data`` — typed payload (a Pydantic model or dict).

Optional extensions used by Mandala:

* ``traceparent`` / ``tracestate`` — W3C Trace Context propagation.
* ``mandalaschemaversion`` — version of the canonical schema (e.g. ``"0.1"``).
* ``mandalaingestid`` — id of the raw inbound webhook (idempotency).
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mandala.core.events.types import EventType

SCHEMA_VERSION = "0.1"
SPEC_VERSION = "1.0"


class MandalaEvent(BaseModel):
    """A CloudEvents 1.0 envelope, validated for Mandala usage."""

    model_config = ConfigDict(populate_by_name=True)

    # --- CloudEvents required attributes ----------------------------------
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str
    type: str  # str (not EventType) so unknown subtypes round-trip cleanly
    specversion: str = SPEC_VERSION
    # --- CloudEvents optional attributes ----------------------------------
    time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    subject: str | None = None
    datacontenttype: str = "application/json"
    dataschema: str | None = None
    # --- Mandala extensions -----------------------------------------------
    mandalaschemaversion: str = SCHEMA_VERSION
    mandalaingestid: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None
    # --- Payload ----------------------------------------------------------
    data: Any = None

    @field_validator("specversion")
    @classmethod
    def _check_specversion(cls, v: str) -> str:
        if v != SPEC_VERSION:
            raise ValueError(f"unsupported CloudEvents specversion: {v!r}")
        return v

    def to_json(self) -> str:
        """Serialize to a JSON string suitable for the bus."""
        return self.model_dump_json(by_alias=True, exclude_none=True)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")

    @classmethod
    def from_json(cls, raw: str | bytes) -> Self:
        return cls.model_validate(json.loads(raw))


def new_event(
    *,
    type: EventType | str,
    source: str,
    subject: str | None = None,
    data: Any = None,
    ingest_id: str | None = None,
    traceparent: str | None = None,
) -> MandalaEvent:
    """Construct a :class:`MandalaEvent` with sensible defaults.

    Args:
        type: A value from :class:`EventType` or a string for forward-compat
            with future types.
        source: Producer URI (e.g. ``"mandala/connector/samsara"``).
        subject: Canonical URN (see :mod:`mandala.core.schema.identifiers`).
        data: Payload — a Pydantic model, dict, or JSON-serializable value.
        ingest_id: ID of the raw inbound webhook for idempotency.
        traceparent: W3C trace context for cross-service correlation.
    """
    if isinstance(data, BaseModel):
        # Use ``mode="json"`` so datetimes etc. are pre-serialized.
        data = data.model_dump(mode="json", exclude_none=True)
    return MandalaEvent(
        type=str(type),
        source=source,
        subject=subject,
        data=data,
        mandalaingestid=ingest_id,
        traceparent=traceparent,
    )
