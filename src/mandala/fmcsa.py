"""FMCSA enrichment detector.

Pure(ish) async function that enriches carrier events with FMCSA SAFER data.
When a DOT number is present on a carrier event, this detector fetches the
live FMCSA record and emits a CARRIER_FMCSA_ENRICHED event.

Debounced to avoid repeated API calls for the same DOT number within a 24h window.
"""

from __future__ import annotations

import structlog

from mandala.connectors.fmcsa.client import FMCSAClient
from mandala.connectors.fmcsa.normalize import (
    enrich_carrier_with_fmcsa as _build_enriched_event,
)
from mandala.core.events.envelope import MandalaEvent, new_event

log = structlog.get_logger(__name__)

_ENRICHMENT_TTL = 86_400  # 24 hours: FMCSA scores update monthly, so cache aggressively


async def _debounce(redis: object, key: str, ttl: int = _ENRICHMENT_TTL) -> bool:
    return bool(await redis.set(f"mandala:fmcsa:enrich:{key}", "1", nx=True, ex=ttl))  # type: ignore[attr-defined]


async def _fetch_fmcsa_data(dot_number: str) -> tuple[dict | None, Exception | None]:
    """Fetch FMCSA data for a single DOT number.

    Returns ``(fmcsa_data, exc)`` where exactly one is ``None``.
    """
    try:
        async with FMCSAClient() as client:
            data = await client.get_carrier_by_dot(dot_number)
            return data, None
    except Exception as exc:  # noqa: BLE001
        log.exception("fmcsa.fetch_failed", dot_number=dot_number, error=str(exc))
        return None, exc


def _failure_event(event: MandalaEvent, dot_number: str, exc: Exception) -> MandalaEvent:
    return new_event(
        type="mandala.fmcsa.enrichment_failed",
        source="mandala/fmcsa",
        subject=event.subject or f"urn:mandala:carrier:{dot_number}",
        data={
            "dot_number": dot_number,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "original_event_type": event.type,
        },
    )


async def enrich_carrier_via_fmcsa(event: MandalaEvent, state: object, redis: object) -> list[MandalaEvent]:
    """Detector: enrich carrier events with FMCSA SAFER data.

    Returns the enriched event on success, or a
    ``mandala.fmcsa.enrichment_failed`` event on transient API failure so
    the warehouse and downstream consumers see the gap explicitly.

    Note on naming: this detector is intentionally named differently from
    :func:`mandala.connectors.fmcsa.normalize.enrich_carrier_with_fmcsa`
    (the pure builder) to avoid the shadow-recursion bug where the
    detector accidentally calls itself instead of the builder.
    """
    data = event.data if isinstance(event.data, dict) else {}

    dot_number = data.get("dot_number") or data.get("carrier_dot") or data.get("fmcsa_dot")
    if not dot_number:
        return []

    # Validate DOT number shape — must be 1-9 digits — to prevent webhook
    # input from being interpolated into outbound URLs as anything weird.
    dot_str = str(dot_number).strip()
    if not dot_str.isdigit() or not 1 <= len(dot_str) <= 9:
        log.warning("fmcsa.skip.invalid_dot", dot_number=dot_number)
        return []

    if not await _debounce(redis, dot_str):
        log.info("fmcsa.skip.debounced", dot_number=dot_str)
        return []

    fmcsa_data, exc = await _fetch_fmcsa_data(dot_str)
    if exc is not None:
        return [_failure_event(event, dot_str, exc)]
    if not fmcsa_data:
        return []

    enriched = _build_enriched_event(event, fmcsa_data)
    log.info(
        "fmcsa.enriched",
        dot_number=dot_str,
        carrier_name=fmcsa_data.get("carrier_name"),
    )
    return [enriched]


DETECTORS = (enrich_carrier_via_fmcsa,)
