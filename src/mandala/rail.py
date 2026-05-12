"""Rail enrichment detector.

Pure(ish) async function that enriches container events with rail status
from Vizion API. When a container ID is present on a shipment event, this
detector fetches the current intermodal status and emits rail events.

Debounced to avoid repeated API calls for the same container within a 1h window.
"""
from __future__ import annotations

import structlog

from mandala.connectors.rail.vizion import VizionRailProvider
from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType

log = structlog.get_logger(__name__)

_ENRICHMENT_TTL = 3_600  # 1 hour: rail status changes infrequently


async def _debounce(redis: object, key: str, ttl: int = _ENRICHMENT_TTL) -> bool:
    return bool(
        await redis.set(f"mandala:rail:enrich:{key}", "1", nx=True, ex=ttl)  # type: ignore[attr-defined]
    )


async def enrich_container_with_rail(
    event: MandalaEvent, state: object, redis: object
) -> list[MandalaEvent]:
    """Enrich container events with rail status from Vizion API when container ID is present.

    This detector checks for container IDs in the event data and fetches the
    corresponding intermodal status from Vizion. Rail events are published back
    to the stream so they flow to the warehouse sink.

    Args:
        event: The MandalaEvent to check for container IDs
        state: StateStore (not used, kept for detector signature compatibility)
        redis: Redis client for debouncing

    Returns:
        List containing the rail events, or empty if no container ID or
        already enriched within TTL.
    """
    data = event.data if isinstance(event.data, dict) else {}

    # Extract container ID from various possible locations
    container_id = data.get("container_id") or data.get("container_number")
    if not container_id:
        return []

    # Skip if we've already enriched this container recently
    if not await _debounce(redis, str(container_id)):
        log.info("rail.skip.debounced", container_id=container_id)
        return []

    try:
        provider = VizionRailProvider()
        if not provider.is_configured():
            log.info("rail.skip.not_configured")
            return []

        intermodal = provider.get_intermodal_status(str(container_id))
    except Exception as exc:  # noqa: BLE001
        log.exception("rail.fetch_failed", container_id=container_id, error=str(exc))
        # Emit a failure event so the warehouse + downstream consumers
        # can see the gap explicitly. The worker publishes whatever the
        # detector returns, so this flows through the normal pipeline.
        return [
            new_event(
                type="mandala.rail.enrichment_failed",
                source="mandala/rail",
                subject=event.subject,
                data={
                    "container_id": str(container_id),
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "original_event_type": event.type,
                },
            )
        ]

    # Emit the intermodal status event
    out = [
        new_event(
            type=EventType.RAIL_INTERMODAL_STATUS,
            source="mandala/connector/rail",
            subject=event.subject,
            data={
                "container_id": intermodal.container_id,
                "carrier_scac": intermodal.carrier_scac,
                "origin_ramp": intermodal.origin_ramp,
                "destination_ramp": intermodal.destination_ramp,
                "last_free_day": intermodal.last_free_day.isoformat() if intermodal.last_free_day else None,
                "available_for_pickup": intermodal.available_for_pickup,
                "eta": intermodal.eta.isoformat() if intermodal.eta else None,
                "provider": intermodal.provider,
                "retrieved_at": intermodal.retrieved_at.isoformat(),
                "milestones": [
                    {
                        "event_type": m.event_type,
                        "location": m.location,
                        "timestamp": m.timestamp.isoformat(),
                        "timezone": m.timezone,
                    }
                    for m in intermodal.milestones
                ],
            },
        )
    ]

    log.info("rail.enriched", container_id=container_id, carrier_scac=intermodal.carrier_scac)
    return out


DETECTORS = (enrich_container_with_rail,)
