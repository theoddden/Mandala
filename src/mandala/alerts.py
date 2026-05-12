"""Alert detectors.

Pure(ish) async functions. Each takes ``(event, state, redis)`` and
returns a list of new events to publish. No classes, no DI framework.

* :func:`cross_border` — fires when a truck enters a Port-of-Entry geofence
  with no linked shipment, or with a customs filing missing/not released.
* :func:`cold_chain` — re-emits ``COLD_CHAIN_BREACH`` annotated with the
  matching shipment's declared min/max temperature, if known.
* :func:`dead_zone` — fires when a truck's position pings drop off, indicating
  a connectivity dead zone. Logs the last known location and gap duration.

Add detectors here; they're cheap.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.state import StateStore

log = structlog.get_logger(__name__)

CROSS_BORDER_NO_FILING = "mandala.alert.cross_border.no_filing"
CROSS_BORDER_HOLD = "mandala.alert.cross_border.hold"
COLD_CHAIN_OUT_OF_SPEC = "mandala.alert.cold_chain.out_of_spec"
DEAD_ZONE_DETECTED = "mandala.connectivity.dead_zone"

_BORDER_TAGS = {"border_poe", "us-mx", "us-ca", "border", "poe"}
_DEBOUNCE_TTL = 1800  # 30 min per truck per fence
_DEAD_ZONE_THRESHOLD_MINUTES = 5  # Configurable per fleet


async def _debounce(redis: object, key: str, ttl: int = _DEBOUNCE_TTL) -> bool:
    return bool(await redis.set(f"mandala:alert:dedup:{key}", "1", nx=True, ex=ttl))  # type: ignore[attr-defined]


def _is_border_poe(data: dict) -> bool:
    name = str(data.get("geofence_name") or "").lower()
    gid = str(data.get("geofence_id") or "").lower()
    return any(t in name or t in gid for t in _BORDER_TAGS)


async def cross_border(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    if event.type != EventType.TRUCK_GEOFENCE_ENTERED.value:
        return []
    data = event.data if isinstance(event.data, dict) else {}
    if not _is_border_poe(data):
        return []

    truck_urn = event.subject or ""
    shipment_urn = await state.shipment_for_truck(truck_urn)
    shipment = await state.get("shipment", shipment_urn) if shipment_urn else None
    customs = (shipment or {}).get("customs_status")
    fence = data.get("geofence_id") or data.get("geofence_name") or "poe"
    if not await _debounce(redis, f"{truck_urn}:{fence}"):
        return []

    if shipment is None:
        reason, severity = "no_linked_shipment", "high"
    elif customs == "hold":
        return [
            new_event(
                type=CROSS_BORDER_HOLD,
                source="mandala/alerts",
                subject=truck_urn,
                data={
                    "truck_urn": truck_urn,
                    "shipment_urn": shipment_urn,
                    "border_poe": data.get("geofence_name"),
                    "severity": "critical",
                },
            )
        ]
    elif customs not in ("filed", "released"):
        reason, severity = "filing_missing_or_not_released", "high"
    else:
        return []

    log.info("cross_border.alert", truck=truck_urn, reason=reason)
    return [
        new_event(
            type=CROSS_BORDER_NO_FILING,
            source="mandala/alerts",
            subject=truck_urn,
            data={
                "truck_urn": truck_urn,
                "shipment_urn": shipment_urn,
                "border_poe": data.get("geofence_name"),
                "customs_status": customs,
                "reason": reason,
                "severity": severity,
            },
        )
    ]


async def cold_chain(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    if event.type != EventType.COLD_CHAIN_BREACH.value:
        return []
    data = event.data if isinstance(event.data, dict) else {}
    truck_urn = event.subject or ""
    shipment_urn = await state.shipment_for_truck(truck_urn)
    shipment = await state.get("shipment", shipment_urn) if shipment_urn else None
    declared_min = (shipment or {}).get("cold_chain_min_c")
    declared_max = (shipment or {}).get("cold_chain_max_c")
    temp = data.get("temperature_c")
    out_of_spec = (declared_max is not None and temp is not None and temp > float(declared_max)) or (
        declared_min is not None and temp is not None and temp < float(declared_min)
    )
    if not (declared_min is None and declared_max is None) and not out_of_spec:
        return []
    if not await _debounce(redis, f"coldchain:{truck_urn}", ttl=600):
        return []
    return [
        new_event(
            type=COLD_CHAIN_OUT_OF_SPEC,
            source="mandala/alerts",
            subject=truck_urn,
            data={
                "truck_urn": truck_urn,
                "shipment_urn": shipment_urn,
                "temperature_c": temp,
                "declared_min_c": declared_min,
                "declared_max_c": declared_max,
                "severity": "high",
            },
        )
    ]


async def dead_zone(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    """Detect when a truck's position pings drop off (connectivity dead zone).

    Uses the Stator's Latch to track last committed time per entity.
    When the gap exceeds the threshold, emits a dead zone event with the
    last known location. This builds an empirically-derived connectivity map.
    """
    # Only trigger on position events
    if event.type != EventType.TRUCK_POSITION.value:
        return []

    truck_urn = event.subject or ""
    if not truck_urn:
        return []

    # Get last committed time from Stator's Latch via Redis
    latch_key = f"mandala:latch:{truck_urn}"
    last_committed_raw = await redis.get(latch_key)  # type: ignore[attr-defined]

    if not last_committed_raw:
        return []  # First event, no baseline

    if isinstance(last_committed_raw, bytes):
        last_committed_raw = last_committed_raw.decode()

    try:
        last_committed = datetime.fromisoformat(last_committed_raw)
    except (ValueError, TypeError):
        return []

    time_gap = (event.time - last_committed).total_seconds()

    if time_gap < (_DEAD_ZONE_THRESHOLD_MINUTES * 60):
        return []  # Normal ping interval

    # This is a dead zone - log the last known location
    data = event.data if isinstance(event.data, dict) else {}
    position = data.get("position") or {}

    # Debounce to prevent spamming dead zone events for the same location
    geo_key = f"{position.get('lat'):.4f},{position.get('lon'):.4f}"
    if not await _debounce(redis, f"deadzone:{truck_urn}:{geo_key}", ttl=3600):
        return []

    log.info(
        "dead_zone.detected",
        truck=truck_urn,
        gap_seconds=time_gap,
        last_known_lat=position.get("lat"),
        last_known_lon=position.get("lon"),
    )

    # Emit dead zone event with spatial metadata
    return [
        new_event(
            type=DEAD_ZONE_DETECTED,
            source="mandala/alerts",
            subject=truck_urn,
            data={
                "truck_urn": truck_urn,
                "last_known_lat": position.get("lat"),
                "last_known_lon": position.get("lon"),
                "gap_duration_seconds": time_gap,
                "gap_start": last_committed.isoformat(),
                "gap_end": event.time.isoformat(),
                "severity": "medium" if time_gap < 3600 else "high",
            },
            attributes={
                "logistics.location.lat": position.get("lat"),
                "logistics.location.lon": position.get("lon"),
                "connectivity.gap_seconds": time_gap,
                "connectivity.dead_zone": "true",
            },
        )
    ]


DETECTORS = (cross_border, cold_chain, dead_zone)
