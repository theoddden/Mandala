"""Convert CargoWise Universal Event XML into :class:`MandalaEvent` objects.

The Universal Event document has this skeletal shape::

    <UniversalEvent xmlns="http://www.cargowise.com/Schemas/Universal/2011/11">
      <Event>
        <DataContext>
          <DataSourceCollection>
            <DataSource>
              <Type>ForwardingShipment</Type>
              <Key>SHIPREF12345</Key>
            </DataSource>
          </DataSourceCollection>
          <Company>
            <Code>ORG-CO</Code>
          </Company>
        </DataContext>
        <EventTime>2026-05-08T22:14:09</EventTime>
        <EventType>DIM</EventType>          <!-- 3-letter status code -->
        <EventReference>Departed origin</EventReference>
      </Event>
    </UniversalEvent>

We map the 3-letter ``EventType`` codes to canonical Mandala event types.
The mapping covers the most common ~20 codes seen on cross-border
shipments; unknown codes pass through as ``mandala.shipment.in_transit``
with the original code preserved in ``data.cargowise_event_type``.

We do **not** parse the full Universal Shipment document here — that
arrives via a separate subscription and gets normalized in a sibling
module if/when needed. v0.1 covers status milestones, customs events,
and BOL receipt, which is the high-frequency stream.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.schema.identifiers import URN

SOURCE = "mandala/connector/cargowise"
NS = {"u": "http://www.cargowise.com/Schemas/Universal/2011/11"}


# CargoWise EventType code → Mandala event type. Codes are the 3-letter
# status codes from the WiseTech "Status Codes" reference; the list below
# is the high-frequency subset. Extend by adding to this dict.
_STATUS_MAP: dict[str, EventType] = {
    # Shipment lifecycle
    "BKD": EventType.SHIPMENT_BOOKED,
    "DIM": EventType.SHIPMENT_DISPATCHED,  # Departed import gateway / movement
    "PUF": EventType.SHIPMENT_PICKED_UP,  # Pick-up from origin
    "ITR": EventType.SHIPMENT_IN_TRANSIT,
    "ARR": EventType.SHIPMENT_AT_BORDER,  # Arrival at gateway
    "POD": EventType.SHIPMENT_DELIVERED,  # Proof of Delivery
    "DLV": EventType.SHIPMENT_DELIVERED,
    "CXL": EventType.SHIPMENT_CANCELLED,
    "ETA": EventType.SHIPMENT_ETA_UPDATED,
    "HND": EventType.SHIPMENT_HANDOFF,  # Handoff confirmed
    # Customs
    "CDF": EventType.CUSTOMS_FILED,  # Customs declaration filed
    "CDH": EventType.CUSTOMS_HOLD,  # Customs hold
    "CDE": EventType.CUSTOMS_EXAM,  # Customs exam
    "CDR": EventType.CUSTOMS_RELEASED,  # Customs released
    "CDX": EventType.CUSTOMS_REJECTED,  # Customs rejected
    # BOL / paperwork
    "BLR": EventType.BOL_RECEIVED,
    "BLA": EventType.BOL_AMENDED,
}


def _txt(elem: ET.Element | None, path: str) -> str | None:
    if elem is None:
        return None
    found = elem.find(path, NS)
    return found.text if found is not None and found.text else None


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _shipment_urn(scope: str, key: str) -> str:
    return str(URN.shipment(scope=scope or "cargowise", id=key))


def _ingest_id(elem: ET.Element) -> str | None:
    # Universal Event documents may carry a TrackingID GUID in DataContext.
    return _txt(elem, "u:Event/u:DataContext/u:EventTrackingID") or _txt(elem, "u:Event/u:EventTrackingID")


def normalize(body: bytes | str) -> list[MandalaEvent]:
    """Convert a Universal Event XML document into Mandala events."""
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []

    # Some senders wrap the UniversalEvent in a UniversalInterchange envelope.
    # Find the first UniversalEvent regardless of depth.
    if root.tag != f"{{{NS['u']}}}UniversalEvent":
        ev = root.find(".//u:UniversalEvent", NS)
        if ev is None:
            return []
        root = ev

    event_elem = root.find("u:Event", NS)
    if event_elem is None:
        return []

    cw_type = (_txt(event_elem, "u:EventType") or "").strip().upper()
    occurred_at = _parse_ts(_txt(event_elem, "u:EventTime"))
    reference = _txt(event_elem, "u:EventReference")

    # DataSource is where the shipment / job key lives. Use the first one
    # whose Type signals a shipment-like record.
    data_source = event_elem.find("u:DataContext/u:DataSourceCollection/u:DataSource", NS)
    ds_type = (_txt(data_source, "u:Type") or "").strip()
    ds_key = (_txt(data_source, "u:Key") or "").strip()
    org_code = _txt(event_elem, "u:DataContext/u:Company/u:Code")

    if not ds_key:
        return []

    subject = _shipment_urn(scope="cargowise", key=ds_key)
    mandala_type = _STATUS_MAP.get(cw_type, EventType.SHIPMENT_IN_TRANSIT)
    payload: dict[str, Any] = {
        "shipment_id": ds_key,
        "data_source_type": ds_type,
        "cargowise_event_type": cw_type,
        "cargowise_event_reference": reference,
        "organization_code": org_code,
        "vendor": "cargowise",
    }
    if occurred_at:
        payload["occurred_at"] = occurred_at.isoformat()

    # Customs hold / exam events typically carry a reason in EventReference.
    if (
        mandala_type
        in {
            EventType.CUSTOMS_HOLD,
            EventType.CUSTOMS_EXAM,
            EventType.CUSTOMS_REJECTED,
        }
        and reference
    ):
        payload["hold_reason"] = reference

    return [
        new_event(
            type=mandala_type,
            source=SOURCE,
            subject=subject,
            data=payload,
            ingest_id=_ingest_id(root),
        )
    ]
