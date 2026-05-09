"""Shipment, customs, and bill-of-lading canonical objects.

Normalized from Descartes (GLN, MacroPoint, Datamyne, Compliance) and any
other trade platform connectors.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from mandala.core.schema.geo import BorderCrossing, GeoPoint
from mandala.core.schema.party import Party


class ShipmentStatus(StrEnum):
    BOOKED = "booked"
    DISPATCHED = "dispatched"
    IN_TRANSIT = "in_transit"
    AT_BORDER = "at_border"
    HELD = "held"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class CustomsStatus(StrEnum):
    NOT_FILED = "not_filed"
    FILED = "filed"
    UNDER_REVIEW = "under_review"
    HOLD = "hold"
    EXAM = "exam"
    RELEASED = "released"
    REJECTED = "rejected"


class TransportMode(StrEnum):
    TRUCK = "truck"
    RAIL = "rail"
    OCEAN = "ocean"
    AIR = "air"
    INTERMODAL = "intermodal"


class HazmatClass(StrEnum):
    NONE = "none"
    CLASS_1_EXPLOSIVES = "1"
    CLASS_2_GASES = "2"
    CLASS_3_FLAMMABLE_LIQUIDS = "3"
    CLASS_4_FLAMMABLE_SOLIDS = "4"
    CLASS_5_OXIDIZERS = "5"
    CLASS_6_TOXIC = "6"
    CLASS_7_RADIOACTIVE = "7"
    CLASS_8_CORROSIVE = "8"
    CLASS_9_MISC = "9"


class CommodityLine(BaseModel):
    """One line item on a shipment / customs entry."""

    model_config = ConfigDict(frozen=True)

    description: str
    hts_code: str | None = Field(
        default=None,
        description="Harmonized Tariff Schedule code (10-digit US HTSUS or 8-digit HS).",
    )
    quantity: Decimal | None = None
    unit: str | None = None
    weight_kg: Decimal | None = None
    value_usd: Decimal | None = None
    country_of_origin: str | None = None  # ISO 3166-1 alpha-2
    hazmat: HazmatClass = HazmatClass.NONE


class CustomsEntry(BaseModel):
    """Customs entry filed against a shipment (CBP, SAT, CBSA, etc.)."""

    model_config = ConfigDict(frozen=True)

    id: str
    shipment_id: str
    authority: str  # cbp | cbsa | sat | ...
    entry_number: str | None = None
    status: CustomsStatus
    filed_at: datetime | None = None
    released_at: datetime | None = None
    hold_reason: str | None = None
    duty_owed_usd: Decimal | None = None
    importer: Party | None = None
    broker: Party | None = None
    lines: list[CommodityLine] = Field(default_factory=list)


class BillOfLading(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    shipment_id: str
    bol_number: str
    issued_at: datetime | None = None
    issuer: Party | None = None
    consignee: Party | None = None
    received_at_destination_at: datetime | None = None
    pdf_url: str | None = None


class ShipmentLeg(BaseModel):
    """One physical leg of a multimodal shipment."""

    model_config = ConfigDict(frozen=True)

    sequence: int
    mode: TransportMode
    carrier: Party | None = None
    truck_urn: str | None = None  # canonical URN linking to a Truck
    origin: GeoPoint | None = None
    destination: GeoPoint | None = None
    border_crossing: BorderCrossing | None = None
    planned_departure_at: datetime | None = None
    planned_arrival_at: datetime | None = None
    actual_departure_at: datetime | None = None
    actual_arrival_at: datetime | None = None


class ShipmentMilestone(BaseModel):
    """A discrete event on a shipment timeline (booked, picked-up, at-border, ...)."""

    model_config = ConfigDict(frozen=True)

    shipment_id: str
    name: str
    status: ShipmentStatus | None = None
    occurred_at: datetime
    location: GeoPoint | None = None
    note: str | None = None


class Shipment(BaseModel):
    """Canonical shipment object — the bridge format between all connectors."""

    id: str
    reference: str | None = None  # e.g. customer PO
    status: ShipmentStatus = ShipmentStatus.BOOKED
    customs_status: CustomsStatus = CustomsStatus.NOT_FILED

    shipper: Party | None = None
    consignee: Party | None = None
    carrier: Party | None = None
    broker: Party | None = None

    legs: list[ShipmentLeg] = Field(default_factory=list)
    commodities: list[CommodityLine] = Field(default_factory=list)
    customs_entry: CustomsEntry | None = None
    bills_of_lading: list[BillOfLading] = Field(default_factory=list)

    eta: datetime | None = None
    eta_confidence: float | None = Field(default=None, ge=0, le=1)
    last_position: GeoPoint | None = None

    cold_chain_required: bool = False
    cold_chain_min_c: float | None = None
    cold_chain_max_c: float | None = None

    metadata: dict[str, str] = Field(default_factory=dict)
    updated_at: datetime | None = None
