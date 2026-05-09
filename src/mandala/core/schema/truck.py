"""Truck / driver / telemetry canonical objects.

Source data normalized from Samsara (and future telematics connectors:
Geotab, Motive, Verizon Connect, etc.). Shape is vendor-agnostic.
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from mandala.core.schema.geo import GeoPoint


class EquipmentType(StrEnum):
    """Trailer / equipment types as canonicalized for load-board postings.

    Maps to DAT's equipment codes and Truckstop's equipment-type IDs.
    Connectors are responsible for translating to the vendor-specific code.
    """

    VAN = "van"                       # 53' dry van
    REEFER = "reefer"                 # refrigerated
    FLATBED = "flatbed"
    STEPDECK = "stepdeck"
    DOUBLE_DROP = "double_drop"
    LOWBOY = "lowboy"
    POWER_ONLY = "power_only"
    CONTAINER = "container"           # intermodal
    HOTSHOT = "hotshot"
    AUTO_CARRIER = "auto_carrier"
    TANKER = "tanker"
    BOX_TRUCK = "box_truck"
    OTHER = "other"


class FuelType(StrEnum):
    DIESEL = "diesel"
    GASOLINE = "gasoline"
    ELECTRIC = "electric"
    HYBRID = "hybrid"
    LNG = "lng"
    CNG = "cng"
    HYDROGEN = "hydrogen"


class Driver(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    license_number: str | None = None
    license_country: str | None = None
    phone: str | None = None
    hos_remaining_min: int | None = Field(
        default=None,
        description="Hours-of-service driving time remaining, in minutes.",
    )


class Truck(BaseModel):
    """Canonical truck / tractor."""

    model_config = ConfigDict(frozen=True)

    id: str  # vendor-scoped id
    vin: str | None = None
    license_plate: str | None = None
    license_plate_country: str | None = None
    make: str | None = None
    model: str | None = None
    year: int | None = None
    fuel_type: FuelType | None = None
    has_reefer: bool = False
    equipment: EquipmentType | None = None
    length_ft: float | None = None
    tags: list[str] = Field(default_factory=list)


class TruckPosition(BaseModel):
    """A single position fix."""

    model_config = ConfigDict(frozen=True)

    truck_id: str
    point: GeoPoint
    odometer_km: float | None = Field(default=None, ge=0)
    fuel_pct: float | None = Field(default=None, ge=0, le=100)
    soc_pct: float | None = Field(default=None, ge=0, le=100, description="EV battery state-of-charge.")
    engine_state: str | None = None  # idle | running | off
    captured_at: datetime


class ColdChainReading(BaseModel):
    """Reefer / cold chain sensor reading."""

    model_config = ConfigDict(frozen=True)

    truck_id: str
    sensor_id: str
    temperature_c: float
    humidity_pct: float | None = Field(default=None, ge=0, le=100)
    setpoint_c: float | None = None
    door_open: bool | None = None
    captured_at: datetime


class TruckTelemetry(BaseModel):
    """Combined telemetry envelope used inside CloudEvent ``data``."""

    truck: Truck
    position: TruckPosition | None = None
    cold_chain: list[ColdChainReading] = Field(default_factory=list)
    driver: Driver | None = None
