"""Project canonical events into the Redis state store.

This is the only place that mutates :class:`StateStore`. Detectors and the
MCP server read from it but never write.

To prevent vendor payload bugs from corrupting state, each projection uses
an explicit field allowlist. Only fields known to be safe are merged into
the stored object.
"""
from __future__ import annotations

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType
from mandala.core.state import StateStore

# Map event types -> state mutations.
_SHIPMENT_STATUS_EVENTS = {
    EventType.SHIPMENT_BOOKED.value: "booked",
    EventType.SHIPMENT_DISPATCHED.value: "dispatched",
    EventType.SHIPMENT_PICKED_UP.value: "in_transit",
    EventType.SHIPMENT_IN_TRANSIT.value: "in_transit",
    EventType.SHIPMENT_AT_BORDER.value: "at_border",
    EventType.SHIPMENT_DELIVERED.value: "delivered",
    EventType.SHIPMENT_CANCELLED.value: "cancelled",
}

_CUSTOMS_STATUS_EVENTS = {
    EventType.CUSTOMS_FILED.value: "filed",
    EventType.CUSTOMS_HOLD.value: "hold",
    EventType.CUSTOMS_EXAM.value: "exam",
    EventType.CUSTOMS_RELEASED.value: "released",
    EventType.CUSTOMS_REJECTED.value: "rejected",
}

# Explicit field allowlists to prevent vendor payload bugs from corrupting state.
_SHIPMENT_STATUS_FIELDS = {
    "carrier_name",
    "origin_address",
    "destination_address",
    "pickup_appointment",
    "delivery_appointment",
    "weight_lb",
    "volume_cft",
    "commodity",
    "reference_number",
    "bol_number",
}

_CUSTOMS_STATUS_FIELDS = {
    "filing_number",
    "filing_timestamp",
    "port_of_entry",
    "exam_type",
    "hold_reason",
}

_TRUCK_POSITION_FIELDS = {
    "last_position",
    "last_seen_at",
    "vin",
    "license_plate",
    "equipment_type",
}


async def project(event: MandalaEvent, state: StateStore) -> None:
    data = event.data if isinstance(event.data, dict) else {}
    subject = event.subject or ""

    if event.type in _SHIPMENT_STATUS_EVENTS and subject.startswith("urn:mandala:shipment:"):
        patch = {"status": _SHIPMENT_STATUS_EVENTS[event.type]}
        for k in _SHIPMENT_STATUS_FIELDS:
            if k in data and data[k] is not None:
                patch[k] = data[k]
        await state.upsert("shipment", subject, patch)
        await state.append_timeline(subject, {"type": event.type, "at": event.time.isoformat(), "data": data})
        return

    if event.type in _CUSTOMS_STATUS_EVENTS and subject.startswith("urn:mandala:shipment:"):
        patch = {"customs_status": _CUSTOMS_STATUS_EVENTS[event.type]}
        for k in _CUSTOMS_STATUS_FIELDS:
            if k in data and data[k] is not None:
                patch[k] = data[k]
        await state.upsert("shipment", subject, patch)
        await state.append_timeline(subject, {"type": event.type, "at": event.time.isoformat(), "data": data})
        return

    if event.type == EventType.SHIPMENT_ETA_UPDATED.value and subject.startswith("urn:mandala:shipment:"):
        eta = data.get("eta")
        if eta is not None:
            await state.upsert("shipment", subject, {"eta": eta})
        return

    if event.type == EventType.TRUCK_POSITION.value and subject.startswith("urn:mandala:truck:"):
        pos = data.get("position") or {}
        truck = data.get("truck") or {}
        patch = {
            "last_position": pos.get("point"),
            "last_seen_at": pos.get("captured_at"),
            "vin": truck.get("vin"),
            "license_plate": truck.get("license_plate"),
            "equipment_type": truck.get("equipment_type"),
        }
        # Filter out None values
        patch = {k: v for k, v in patch.items() if v is not None}
        await state.upsert("truck", subject, patch)

    if event.type == EventType.SHIPMENT_HANDOFF.value:
        truck_urn = data.get("truck_urn")
        shipment_urn = data.get("shipment_urn") or subject
        # Validate URN prefixes to prevent malformed events from creating bogus links
        if truck_urn and shipment_urn:
            if not truck_urn.startswith("urn:mandala:truck:"):
                return
            if not shipment_urn.startswith("urn:mandala:shipment:"):
                return
            await state.link(truck_urn, shipment_urn)
