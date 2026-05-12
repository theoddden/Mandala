"""Normalize FMCSA SAFER API responses into Mandala event enrichment.

FMCSA data is used for enrichment only — it decorates existing carrier
events with safety profile data. This module provides pure functions to
transform the raw SAFER API response into the canonical FMCSA enrichment
shape that attaches to carrier events.
"""

from __future__ import annotations

from typing import Any

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType


def enrich_carrier_with_fmcsa(
    event: MandalaEvent,
    fmcsa_data: dict[str, Any],
) -> MandalaEvent:
    """Decorate a carrier event with FMCSA safety profile data.

    This function creates a new event that carries the FMCSA enrichment
    data. The original event is preserved; the enriched event is published
    back to the stream so it flows to the warehouse sink.

    Args:
        event: The original MandalaEvent (e.g. mandala.carrier.created)
        fmcsa_data: Raw SAFER API response from FMCSAClient.get_carrier_by_dot

    Returns:
        A new MandalaEvent of type mandala.carrier.fmcsa_enriched with the
        FMCSA data in the payload.
    """
    data = event.data if isinstance(event.data, dict) else {}

    return new_event(
        type=EventType.CARRIER_FMCSA_ENRICHED,
        source="mandala/connector/fmcsa",
        subject=event.subject,
        data={
            "carrier_urn": event.subject,
            "original_event_type": event.type,
            "original_data": data,
            "fmcsa": fmcsa_data,
        },
    )
