"""Abstract rail provider protocol — implement this for any rail carrier."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass
class RailMilestone:
    event_type: str  # e.g. "INGATE", "OUTGATE", "ARRIVAL", "DEPARTURE"
    location: str  # yard/ramp name
    timestamp: datetime
    timezone: str
    raw: dict  # original provider payload, unmodified


@dataclass
class IntermodalEvent:
    container_id: str
    carrier_scac: str  # rail carrier SCAC e.g. "BNSF", "UPGF"
    origin_ramp: str | None
    destination_ramp: str | None
    last_free_day: datetime | None
    available_for_pickup: bool
    eta: datetime | None
    milestones: list[RailMilestone]
    provider: str  # "vizion", "bnsf_direct", etc.
    retrieved_at: datetime


@runtime_checkable
class RailProvider(Protocol):
    """Implement this protocol to add any rail tracking source to Mandala."""

    def get_intermodal_status(self, container_id: str) -> IntermodalEvent:
        """Return current intermodal status for a container."""
        ...

    def is_configured(self) -> bool:
        """Return True if the provider has valid credentials."""
        ...
