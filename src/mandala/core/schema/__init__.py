"""Canonical Mandala domain schema.

These Pydantic models are the *bridge format*: every connector normalizes
incoming data into these objects, and every outbound playbook action
operates on these objects. They are independent of any single vendor's
data model.
"""
from mandala.core.schema.geo import BorderCrossing, Geofence, GeoPoint
from mandala.core.schema.identifiers import URN, parse_urn
from mandala.core.schema.party import Address, Party
from mandala.core.schema.shipment import (
    BillOfLading,
    CustomsEntry,
    CustomsStatus,
    Shipment,
    ShipmentLeg,
    ShipmentMilestone,
    ShipmentStatus,
)
from mandala.core.schema.truck import (
    ColdChainReading,
    Driver,
    EquipmentType,
    FuelType,
    Truck,
    TruckPosition,
    TruckTelemetry,
)

__all__ = [
    "GeoPoint",
    "Geofence",
    "BorderCrossing",
    "Party",
    "Address",
    "Shipment",
    "ShipmentLeg",
    "ShipmentMilestone",
    "ShipmentStatus",
    "CustomsStatus",
    "CustomsEntry",
    "BillOfLading",
    "Truck",
    "TruckTelemetry",
    "TruckPosition",
    "ColdChainReading",
    "Driver",
    "EquipmentType",
    "FuelType",
    "URN",
    "parse_urn",
]
