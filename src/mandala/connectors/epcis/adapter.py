"""EPCIS 2.0 adapter for GS1 compliance.

GS1 EPCIS 2.0 is a global standard for capturing and sharing event-level
supply chain data. This adapter converts MandalaEvents to EPCIS 2.0 JSON
format and emits them to EPCIS capture endpoints.

See docs/standards/epcis.md for full integration pattern.
"""
from __future__ import annotations

import httpx
from datetime import UTC, datetime

from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings


class EPCISAdapter:
    """EPCIS 2.0 adapter for GS1 compliance.

    Converts MandalaEvents to EPCIS 2.0 JSON format and captures them
    to EPCIS endpoints. Makes Mandala compatible with every GS1 EPCIS
    subscriber globally.
    """

    def __init__(self) -> None:
        s = get_settings()
        self.capture_url = s.epcis_capture_url
        self.query_url = s.epcis_query_url
        self.gln = s.epcis_gln  # GS1 Global Location Number

    def to_epcis_event(self, event: MandalaEvent) -> dict:
        """Convert MandalaEvent to EPCIS 2.0 JSON format.

        EPCIS 2.0 event structure:
        - eventTime: RFC 3339 timestamp
        - eventType: ObjectEvent, AggregationEvent, TransactionEvent, etc.
        - action: OBSERVE, ADD, DELETE, etc.
        - bizStep: shipping, receiving, etc.
        - disposition: in_transit, in_progress, etc.
        - readPoint: location where event occurred (GLN)
        - epcList: list of EPCs (SSCC, GTIN, etc.)
        - sensorElementList: sensor data (temperature, humidity, etc.)
        """
        data = event.data if isinstance(event.data, dict) else {}

        # Determine EPCIS event type based on Mandala event type
        event_type_mapping = {
            "mandala.shipment.delivered": "ObjectEvent",
            "mandala.shipment.loaded": "ObjectEvent",
            "mandala.shipment.unloaded": "ObjectEvent",
            "mandala.truck.location.updated": "ObjectEvent",
            "mandala.cold_chain.breach": "ObjectEvent",
        }
        epcis_event_type = event_type_mapping.get(event.type, "ObjectEvent")

        # Determine action based on event type
        action_mapping = {
            "mandala.shipment.delivered": "OBSERVE",
            "mandala.shipment.loaded": "ADD",
            "mandala.shipment.unloaded": "DELETE",
        }
        action = action_mapping.get(event.type, "OBSERVE")

        # Determine business step
        biz_step_mapping = {
            "mandala.shipment.delivered": "receiving",
            "mandala.shipment.loaded": "loading",
            "mandala.shipment.unloaded": "unloading",
            "mandala.truck.location.updated": "transporting",
        }
        biz_step = biz_step_mapping.get(event.type, "shipping")

        # Determine disposition
        disposition_mapping = {
            "mandala.shipment.delivered": "in_progress",
            "mandala.shipment.loaded": "in_transit",
            "mandala.shipment.unloaded": "in_progress",
        }
        disposition = disposition_mapping.get(event.type, "in_transit")

        # Build EPCIS event
        epcis_event: dict[str, object] = {
            "eventTime": event.time.astimezone(UTC).isoformat(),
            "eventType": epcis_event_type,
            "action": action,
            "bizStep": biz_step,
            "disposition": disposition,
            "recordTime": datetime.now(UTC).isoformat(),
        }

        # Add readPoint (location) if available
        if data.get("location_id") or data.get("latitude"):
            location_id = data.get("location_id") or self.gln
            epcis_event["readPoint"] = {
                "id": f"urn:epc:id:sgln:{location_id}"
            }

        # Add epcList (SSCC, GTIN) if available
        epc_list = []
        if data.get("sscc"):
            epc_list.append(f"urn:epc:id:sscc:{data['sscc']}")
        if data.get("gtin"):
            epc_list.append(f"urn:epc:id:gtin:{data['gtin']}")
        if data.get("shipment_urn"):
            # Extract SSCC from shipment URN if present
            epc_list.append(f"urn:epc:id:sscc:{data['shipment_urn']}")
        if epc_list:
            epcis_event["epcList"] = epc_list

        # Add sensorElementList (temperature, humidity, etc.) if available
        sensor_elements = []
        if data.get("temperature_celsius"):
            sensor_elements.append({
                "type": "Temperature",
                "measurement": {
                    "value": data["temperature_celsius"],
                    "uom": "CEL"
                }
            })
        if data.get("humidity_percent"):
            sensor_elements.append({
                "type": "Humidity",
                "measurement": {
                    "value": data["humidity_percent"],
                    "uom": "PCT"
                }
            })
        if sensor_elements:
            epcis_event["sensorElementList"] = sensor_elements

        # Add extension fields for Mandala-specific data
        epcis_event["extension"] = {
            "mandala": {
                "event_id": event.id,
                "source": event.source,
                "subject": event.subject,
            }
        }

        return epcis_event

    async def capture(self, event: MandalaEvent) -> bool:
        """Capture MandalaEvent to EPCIS endpoint.

        Returns True if successful, False otherwise.
        """
        if not self.capture_url:
            return False

        epcis_event = self.to_epcis_event(event)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.capture_url}/capture",
                    json=epcis_event,
                    headers={
                        "GS1-EPCIS-Capture-Error-Behavior": "rollback",
                        "GS1-EPCIS-Format": "JSON",
                    },
                    timeout=30.0,
                )
                response.raise_for_status()
                return True
        except Exception:
            return False

    def is_configured(self) -> bool:
        """Check if EPCIS adapter is configured."""
        return bool(self.capture_url and self.gln)
