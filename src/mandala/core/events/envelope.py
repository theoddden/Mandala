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
* ``mandalaidempotencykey`` — SHA256-derived idempotency key for exactly-once delivery.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mandala.core.events.types import EventType

SCHEMA_VERSION = "0.3"
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
    time: datetime = Field(default_factory=lambda: datetime.now(UTC))  # when the physical event OCCURRED
    subject: str | None = None
    datacontenttype: str = "application/json"
    dataschema: str | None = None
    # --- Mandala extensions -----------------------------------------------
    mandalaschemaversion: str = SCHEMA_VERSION
    mandalaingestid: str | None = None
    mandalaidempotencykey: str | None = Field(default=None, description="SHA256-derived idempotency key for exactly-once delivery")
    traceparent: str | None = None
    tracestate: str | None = None
    # --- Three-timestamp accounting ----------------------------------------
    received_at: datetime | None = Field(default=None, description="When Mandala's webhook received the event")
    processed_at: datetime | None = Field(default=None, description="When the worker ran detectors on the event")
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

    def compute_idempotency_key(self) -> str:
        """Compute deterministic idempotency key from source payload.
        
        Key is SHA256(vendor + event_type + occurred_at + entity_id).
        This ensures exactly-once delivery by detecting duplicate events
        from webhook retries or network hiccups.
        """
        # Extract vendor from source (e.g., "mandala/connector/samsara" -> "samsara")
        vendor = self.source.split("/")[-1] if "/" in self.source else self.source
        
        # Extract entity_id from subject if available
        entity_id = self.subject if self.subject else ""
        
        # Build key components
        key_components = f"{vendor}:{self.type}:{self.time.isoformat()}:{entity_id}"
        
        # Compute SHA256 hash
        return hashlib.sha256(key_components.encode()).hexdigest()


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
