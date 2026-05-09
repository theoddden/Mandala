"""Trade parties: shippers, consignees, carriers, brokers."""
from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class PartyRole(StrEnum):
    SHIPPER = "shipper"
    CONSIGNEE = "consignee"
    CARRIER = "carrier"
    BROKER = "broker"
    NOTIFY = "notify"
    IMPORTER_OF_RECORD = "importer_of_record"
    EXPORTER_OF_RECORD = "exporter_of_record"


class Address(BaseModel):
    model_config = ConfigDict(frozen=True)

    line1: str
    line2: str | None = None
    city: str
    region: str | None = None  # state / province
    postal_code: str | None = None
    country: str  # ISO 3166-1 alpha-2


class Party(BaseModel):
    """A counter-party in a shipment."""

    model_config = ConfigDict(frozen=True)

    id: str  # vendor-scoped id; the canonical URN is built externally
    name: str
    role: PartyRole
    address: Address | None = None
    tax_id: str | None = None
    eori: str | None = None  # EU
    duns: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    flags: list[str] = Field(
        default_factory=list,
        description="Compliance flags applied by Descartes Compliance / OFAC screening.",
    )
    # FMCSA enrichment (carrier-only, optional)
    dot_number: str | None = None
    fmcsa_safety_rating: str | None = None
    fmcsa_csa_score_max: float | None = None
