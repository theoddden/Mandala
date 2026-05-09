"""Project canonical events into the Redis state store.

This is the only place that mutates :class:`StateStore`. Detectors and the
MCP server read from it but never write.
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


async def project(event: MandalaEvent, state: StateStore) -> None:
    data = event.data if isinstance(event.data, dict) else {}
    subject = event.subject or ""

    if event.type in _SHIPMENT_STATUS_EVENTS and subject.startswith("urn:mandala:shipment:"):
        await state.upsert(
            "shipment",
            subject,
            {"status": _SHIPMENT_STATUS_EVENTS[event.type], **{k: v for k, v in data.items() if k != "status"}},
        )
        await state.append_timeline(subject, {"type": event.type, "at": event.time.isoformat(), "data": data})
        return

    if event.type in _CUSTOMS_STATUS_EVENTS and subject.startswith("urn:mandala:shipment:"):
        await state.upsert(
            "shipment",
            subject,
            {"customs_status": _CUSTOMS_STATUS_EVENTS[event.type], **data},
        )
        await state.append_timeline(subject, {"type": event.type, "at": event.time.isoformat(), "data": data})
        return

    if event.type == EventType.SHIPMENT_ETA_UPDATED.value and subject.startswith("urn:mandala:shipment:"):
        await state.upsert("shipment", subject, {"eta": data.get("eta")})
        return

    if event.type == EventType.TRUCK_POSITION.value and subject.startswith("urn:mandala:truck:"):
        # Just keep last known position; full history lives in the warehouse.
        pos = data.get("position") or {}
        truck = data.get("truck") or {}
        await state.upsert(
            "truck",
            subject,
            {
                "last_position": pos.get("point"),
                "last_seen_at": pos.get("captured_at"),
                "vin": truck.get("vin"),
                "license_plate": truck.get("license_plate"),
            },
        )

    if event.type == EventType.SHIPMENT_HANDOFF.value:
        truck_urn = data.get("truck_urn")
        shipment_urn = data.get("shipment_urn") or subject
        if truck_urn and shipment_urn:
            await state.link(truck_urn, shipment_urn)
