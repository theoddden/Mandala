"""FMCSA enrichment detector.

Pure(ish) async function that enriches carrier events with FMCSA SAFER data.
When a DOT number is present on a carrier event, this detector fetches the
live FMCSA record and emits a CARRIER_FMCSA_ENRICHED event.

Debounced to avoid repeated API calls for the same DOT number within a 24h window.
"""
from __future__ import annotations

import contextlib

import structlog

from mandala.connectors.fmcsa.client import FMCSAClient
from mandala.connectors.fmcsa.normalize import enrich_carrier_with_fmcsa
from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType

log = structlog.get_logger(__name__)

_ENRICHMENT_TTL = 86_400  # 24 hours: FMCSA scores update monthly, so cache aggressively


async def _debounce(redis: "object", key: str, ttl: int = _ENRICHMENT_TTL) -> bool:
    return bool(
        await redis.set(f"mandala:fmcsa:enrich:{key}", "1", nx=True, ex=ttl)  # type: ignore[attr-defined]
    )


async def enrich_carrier_with_fmcsa(
    event: MandalaEvent, state: "object", redis: "object"
) -> list[MandalaEvent]:
    """Enrich carrier events with FMCSA SAFER data when DOT number is present.

    This detector checks for DOT numbers in the event data and fetches the
    corresponding FMCSA safety profile. The enriched event is published back
    to the stream so it flows to the warehouse sink.

    Args:
        event: The MandalaEvent to check for DOT numbers
        state: StateStore (not used, kept for detector signature compatibility)
        redis: Redis client for debouncing

    Returns:
        List containing the enriched event, or empty if no DOT number or
        already enriched within TTL.
    """
    data = event.data if isinstance(event.data, dict) else {}

    # Extract DOT number from various possible locations
    dot_number = data.get("dot_number") or data.get("carrier_dot") or data.get("fmcsa_dot")
    if not dot_number:
        return []

    # Skip if we've already enriched this DOT number recently
    if not await _debounce(redis, str(dot_number)):
        log.info("fmcsa.skip.debounced", dot_number=dot_number)
        return []

    try:
        async with FMCSAClient() as client:
            fmcsa_data = await client.get_carrier_by_dot(str(dot_number))
    except Exception as exc:  # noqa: BLE001
        log.exception("fmcsa.fetch_failed", dot_number=dot_number, error=str(exc))
        return []

    # Create the enriched event
    enriched = enrich_carrier_with_fmcsa(event, fmcsa_data)
    log.info("fmcsa.enriched", dot_number=dot_number, carrier_name=fmcsa_data.get("carrier_name"))

    return [enriched]


DETECTORS = (enrich_carrier_with_fmcsa,)
