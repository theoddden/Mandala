"""Normalize MacroPoint tracking-request and location-update payloads.

MacroPoint's public carrier integration uses a JSON envelope with a
``MessageType`` field. The two messages we care about for v1 are:

* ``TrackingRequest`` — MacroPoint asks the carrier to begin tracking a
  shipment, identified by ``ShipmentId`` and ``OrderNumber``.
* ``StatusUpdate`` — MacroPoint pushes a status change (booked, dispatched,
  in-transit, delivered, exception) on a tracked shipment.

We translate both into events on the canonical Mandala bus.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.schema.identifiers import URN
from mandala.core.schema.shipment import ShipmentStatus

SOURCE = "mandala/connector/descartes-macropoint"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _shipment_urn(shipment_id: Any) -> str:
    return str(URN.shipment(scope="macropoint", id=str(shipment_id)))


_STATUS_TO_EVENT: dict[str, EventType] = {
    "Booked": EventType.SHIPMENT_BOOKED,
    "Dispatched": EventType.SHIPMENT_DISPATCHED,
    "PickedUp": EventType.SHIPMENT_PICKED_UP,
    "InTransit": EventType.SHIPMENT_IN_TRANSIT,
    "AtBorder": EventType.SHIPMENT_AT_BORDER,
    "Delivered": EventType.SHIPMENT_DELIVERED,
    "Cancelled": EventType.SHIPMENT_CANCELLED,
    # Granular customs status events (for real-time customs visibility alerts)
    # These can be emitted by MacroPoint when customs status changes
    "CustomsHoldLanded": EventType.CUSTOMS_HOLD_LANDED,
    "CustomsHoldCleared": EventType.CUSTOMS_HOLD_CLEARED,
    "CustomsDocumentationMissing": EventType.CUSTOMS_DOCUMENTATION_MISSING,
    "CustomsInspectionRequired": EventType.CUSTOMS_INSPECTION_REQUIRED,
}


_STATUS_ENUM_MAP: dict[str, ShipmentStatus] = {
    "Booked": ShipmentStatus.BOOKED,
    "Dispatched": ShipmentStatus.DISPATCHED,
    "PickedUp": ShipmentStatus.IN_TRANSIT,
    "InTransit": ShipmentStatus.IN_TRANSIT,
    "AtBorder": ShipmentStatus.AT_BORDER,
    "Delivered": ShipmentStatus.DELIVERED,
    "Cancelled": ShipmentStatus.CANCELLED,
}


def _ingest_id(payload: dict[str, Any]) -> str | None:
    return payload.get("MessageId") or payload.get("messageId")


def normalize(payload: dict[str, Any]) -> list[MandalaEvent]:
    """Convert a MacroPoint webhook payload into normalized events."""
    msg_type = payload.get("MessageType") or payload.get("messageType") or ""
    body = payload.get("Body") or payload.get("body") or payload

    shipment_id = body.get("ShipmentId") or body.get("shipmentId") or body.get("OrderNumber") or body.get("orderNumber")
    if shipment_id is None:
        return []

    if msg_type == "TrackingRequest":
        return [
            new_event(
                type=EventType.SHIPMENT_BOOKED,
                source=SOURCE,
                subject=_shipment_urn(shipment_id),
                data={
                    "shipment_id": str(shipment_id),
                    "order_number": body.get("OrderNumber") or body.get("orderNumber"),
                    "carrier_scac": body.get("CarrierScac") or body.get("carrierScac"),
                    "origin": body.get("Origin") or body.get("origin"),
                    "destination": body.get("Destination") or body.get("destination"),
                    "pickup_window_start": body.get("PickupWindowStart"),
                    "delivery_window_start": body.get("DeliveryWindowStart"),
                    "vendor": "macropoint",
                },
                ingest_id=_ingest_id(payload),
            )
        ]

    if msg_type in ("StatusUpdate", "LocationUpdate"):
        status_str = body.get("Status") or body.get("status") or "InTransit"
        et = _STATUS_TO_EVENT.get(status_str, EventType.SHIPMENT_IN_TRANSIT)
        data: dict[str, Any] = {
            "shipment_id": str(shipment_id),
            "status": _STATUS_ENUM_MAP.get(status_str, ShipmentStatus.IN_TRANSIT).value,
            "occurred_at": _parse_ts(
                body.get("Timestamp") or body.get("timestamp") or payload.get("Timestamp")
            ).isoformat(),
            "vendor": "macropoint",
        }
        if "Latitude" in body and "Longitude" in body:
            data["location"] = {"lat": float(body["Latitude"]), "lon": float(body["Longitude"])}
        if body.get("Eta") or body.get("eta"):
            data["eta"] = body.get("Eta") or body.get("eta")

        # Laredo Vector Stall: Extract context for customs holds
        if status_str in ("CustomsHoldLanded", "CustomsDocumentationMissing", "CustomsInspectionRequired"):
            data.update(
                {
                    "port_of_entry": body.get("PortOfEntry") or body.get("portOfEntry") or "unknown",
                    "filing_number": body.get("FilingNumber") or body.get("filingNumber"),
                    "hold_reason": body.get("HoldReason") or body.get("holdReason") or status_str,
                    "truck_location": body.get("TruckLocation") or body.get("truckLocation") or "unknown",
                    "eta_to_bridge_minutes": body.get("EtaToBridgeMinutes") or body.get("etaToBridgeMinutes"),
                    "estimated_clearance_hours": body.get("EstimatedClearanceHours")
                    or body.get("estimatedClearanceHours"),
                }
            )

        events = [
            new_event(
                type=et,
                source=SOURCE,
                subject=_shipment_urn(shipment_id),
                data=data,
                ingest_id=_ingest_id(payload),
            )
        ]
        if data.get("eta"):
            events.append(
                new_event(
                    type=EventType.SHIPMENT_ETA_UPDATED,
                    source=SOURCE,
                    subject=_shipment_urn(shipment_id),
                    data={
                        "shipment_id": str(shipment_id),
                        "eta": data["eta"],
                        "source": "macropoint",
                    },
                    ingest_id=(_ingest_id(payload) or "") + ":eta" if _ingest_id(payload) else None,
                )
            )
        return events

    return []
