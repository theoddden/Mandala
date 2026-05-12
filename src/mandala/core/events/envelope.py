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

Trace-native model (Mandala 0.3+):

Every event is also an OpenTelemetry-compatible **span**. A shipment's
lifecycle is a distributed trace; each truck/vessel/customs event is a span
on that trace. The ``trace_id`` is derived deterministically from the
event's ``subject`` (e.g. ``urn:mandala:shipment:ABC123``) so all events
for one shipment auto-correlate without coordination.

* ``trace_id`` — 16-byte hex (32 chars). Derived from subject if unset.
* ``span_id`` — 8-byte hex (16 chars). Derived from event ``id`` if unset.
* ``parent_span_id`` — optional causal parent (e.g. ingest event that triggered a detector).
* ``end_time`` — for spans with duration (vessel transit, customs hold).
* ``attributes`` — OTel-style attributes (semantic conventions: ``logistics.*``).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mandala.core.events.types import EventType

# Try to import Rust-accelerated implementations
try:
    from mandala_rust_ext import (
        compute_idempotency_key as rust_compute_idempotency_key,
    )
    from mandala_rust_ext import (
        derive_span_id as rust_derive_span_id,
    )
    from mandala_rust_ext import (
        derive_trace_id as rust_derive_trace_id,
    )
    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False

SCHEMA_VERSION = "0.3"
SPEC_VERSION = "1.0"


def _derive_trace_id(subject: str | None, fallback: str) -> str:
    """Derive a deterministic 16-byte (32 hex char) trace_id.

    All events sharing a subject (e.g. a shipment URN) get the same trace_id,
    so every truck/vessel/customs event for that shipment auto-correlates
    into a single OpenTelemetry trace without coordination.
    """
    seed = subject if subject else fallback
    if _RUST_EXT_AVAILABLE:
        return rust_derive_trace_id(seed)
    return hashlib.sha256(seed.encode()).hexdigest()[:32]


def _derive_span_id(event_id: str) -> str:
    """Derive an 8-byte (16 hex char) span_id from the event id."""
    if _RUST_EXT_AVAILABLE:
        return rust_derive_span_id(event_id)
    return hashlib.sha256(event_id.encode()).hexdigest()[:16]


def _otlp_value(v: Any) -> dict[str, Any]:
    """Encode a Python value as an OTLP AnyValue."""
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, str):
        return {"stringValue": v}
    return {"stringValue": json.dumps(v, default=str)}


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
    mandalaidempotencykey: str | None = Field(
        default=None, description="SHA256-derived idempotency key for exactly-once delivery"
    )
    traceparent: str | None = None
    tracestate: str | None = None
    # --- Three-timestamp accounting ----------------------------------------
    received_at: datetime | None = Field(default=None, description="When Mandala's webhook received the event")
    processed_at: datetime | None = Field(default=None, description="When the worker ran detectors on the event")
    # --- OpenTelemetry span model (trace-native, Mandala 0.3+) -------------
    trace_id: str | None = Field(default=None, description="16-byte hex trace id; derived from subject if unset")
    span_id: str | None = Field(default=None, description="8-byte hex span id; derived from event id if unset")
    parent_span_id: str | None = Field(default=None, description="Causal parent span (e.g. detector → emitted event)")
    end_time: datetime | None = Field(
        default=None, description="Span end time for events with duration (vessel transit, customs hold)"
    )
    attributes: dict[str, Any] = Field(
        default_factory=dict, description="OTel span attributes (logistics.* semantic conventions)"
    )
    # --- Deterministic Event-Time Windowing (Feature 3) --------------------
    geometric_hash: str | None = Field(
        default=None, description="Geometric hash (H3/S2) for spatial idempotency and event-time determinism"
    )
    delta_t_vector: dict[str, Any] | None = Field(
        default=None,
        description="Vector of Delta-T for trajectory analysis (delta_t_seconds, hash_changed, velocity_mps)",
    )
    # --- Payload ----------------------------------------------------------
    data: Any = None

    def model_post_init(self, __context: Any) -> None:
        # Auto-derive trace/span ids so every event is a valid OTel span.
        # Shipment-subjects auto-correlate into a single trace.
        if self.trace_id is None:
            self.trace_id = _derive_trace_id(self.subject, self.id)
        if self.span_id is None:
            self.span_id = _derive_span_id(self.id)

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

    def to_otlp_span(self) -> dict[str, Any]:
        """Serialize this event as an OpenTelemetry span (OTLP JSON shape).

        Returns a dict matching the OTLP protobuf JSON encoding for a single
        span resource. Suitable for shipping to any OTLP-compatible backend
        (Jaeger, Tempo, Honeycomb, Datadog, Grafana Cloud, etc.).

        The shipment-subject → trace_id mapping means every truck/vessel/customs
        event for a single shipment ends up as spans on the same trace.
        """
        start_ns = int(self.time.timestamp() * 1_000_000_000)
        end_ns = int((self.end_time or self.time).timestamp() * 1_000_000_000)

        # Build OTel attributes from declared attributes + minimal envelope facts.
        attrs: dict[str, Any] = {
            "mandala.event.type": self.type,
            "mandala.source": self.source,
            "mandala.schema_version": self.mandalaschemaversion,
        }
        if self.subject:
            attrs["mandala.subject"] = self.subject
        if self.mandalaingestid:
            attrs["mandala.ingest_id"] = self.mandalaingestid
        attrs.update(self.attributes)

        return {
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "parentSpanId": self.parent_span_id or "",
            "name": self.type,
            "kind": 1,  # SPAN_KIND_INTERNAL (1); ingest spans could override to PRODUCER (3)
            "startTimeUnixNano": str(start_ns),
            "endTimeUnixNano": str(end_ns),
            "attributes": [{"key": k, "value": _otlp_value(v)} for k, v in attrs.items()],
            "status": {"code": 1},  # STATUS_CODE_OK; detectors can set ERROR (2)
        }

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

        if _RUST_EXT_AVAILABLE:
            return rust_compute_idempotency_key(vendor, self.type, self.time.isoformat(), entity_id)

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
    parent_span_id: str | None = None,
    end_time: datetime | None = None,
    attributes: dict[str, Any] | None = None,
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
        parent_span_id: Causal parent span (e.g. when a detector emits an event
            in response to another event).
        end_time: Span end time for events with duration (vessel transit,
            customs hold). Defaults to the start time for instantaneous events.
        attributes: OTel-style attributes following ``logistics.*`` semantic
            conventions (see :mod:`mandala.core.events.semconv`).
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
        parent_span_id=parent_span_id,
        end_time=end_time,
        attributes=attributes or {},
    )


# Alias for backward compatibility with tests
# MandalaEnvelope is a wrapper class that holds a MandalaEvent
class MandalaEnvelope:
    """Wrapper class for MandalaEvent for backward compatibility."""

    def __init__(
        self,
        event: MandalaEvent | None = None,
        received_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.event = event
        self.received_at = received_at or datetime.now(UTC)
        self.metadata = metadata or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert envelope to dictionary."""
        return {
            "event": self.event.model_dump() if self.event else None,
            "received_at": self.received_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create envelope from dictionary."""
        event_data = data.get("event")
        event = MandalaEvent(**event_data) if event_data else None
        received_at = datetime.fromisoformat(data.get("received_at")) if data.get("received_at") else None
        metadata = data.get("metadata", {})
        return cls(event=event, received_at=received_at, metadata=metadata)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MandalaEnvelope):
            return False
        return self.event == other.event and self.received_at == other.received_at

    def __repr__(self) -> str:
        return f"MandalaEnvelope(event={self.event}, received_at={self.received_at}, metadata={self.metadata})"

    def __str__(self) -> str:
        return self.__repr__()


# Keep the alias for cases where tests import directly
MandalaEventAlias = MandalaEvent
