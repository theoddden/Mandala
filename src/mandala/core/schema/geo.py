"""Geospatial primitives."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

Latitude = Annotated[float, Field(ge=-90, le=90)]
Longitude = Annotated[float, Field(ge=-180, le=180)]


class GeoPoint(BaseModel):
    """A WGS84 lat/lon pair with an optional timestamp."""

    model_config = ConfigDict(frozen=True)

    lat: Latitude
    lon: Longitude
    altitude_m: float | None = None
    heading_deg: float | None = Field(default=None, ge=0, lt=360)
    speed_mps: float | None = Field(default=None, ge=0)
    captured_at: datetime | None = None


class GeofenceKind(StrEnum):
    YARD = "yard"
    CUSTOMER = "customer"
    BORDER_POE = "border_poe"  # Port of Entry
    PORT = "port"
    WAREHOUSE = "warehouse"
    CUSTOM = "custom"


class Geofence(BaseModel):
    """A named region; circle (center+radius) or polygon (list of points)."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    kind: GeofenceKind = GeofenceKind.CUSTOM
    center: GeoPoint | None = None
    radius_m: float | None = Field(default=None, ge=0)
    polygon: list[GeoPoint] | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class BorderCrossing(StrEnum):
    """Common North American Ports of Entry. Extend as needed."""

    LAREDO_TX = "us-mx:laredo-tx"
    OTAY_MESA_CA = "us-mx:otay-mesa-ca"
    EL_PASO_TX = "us-mx:el-paso-tx"
    NOGALES_AZ = "us-mx:nogales-az"
    PHARR_TX = "us-mx:pharr-tx"
    DETROIT_MI = "us-ca:detroit-mi"
    BUFFALO_NY = "us-ca:buffalo-ny"
    BLAINE_WA = "us-ca:blaine-wa"
