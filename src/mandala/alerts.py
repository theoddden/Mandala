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
    if (declared_min is None and declared_max is None) or not out_of_spec:
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
    if event.type != EventType.TRUCK_POSITION.value:
        return []

    truck_urn = event.subject or ""
    if not truck_urn:
        return []

    latch_key = f"mandala:latch:{truck_urn}"
    last_committed_raw = await redis.get(latch_key)  # type: ignore[attr-defined]

    if not last_committed_raw:
        return []

    if isinstance(last_committed_raw, bytes):
        last_committed_raw = last_committed_raw.decode()

    try:
        last_committed = datetime.fromisoformat(last_committed_raw)
    except (ValueError, TypeError):
        return []

    time_gap = (event.time - last_committed).total_seconds()

    if time_gap < (_DEAD_ZONE_THRESHOLD_MINUTES * 60):
        return []

    data = event.data if isinstance(event.data, dict) else {}
    position = data.get("position") or {}
    lat, lon = position.get("lat"), position.get("lon")
    if lat is None or lon is None:
        return []

    geo_key = f"{lat:.4f},{lon:.4f}"
    if not await _debounce(redis, f"deadzone:{truck_urn}:{geo_key}", ttl=3600):
        return []

    log.info(
        "dead_zone.detected",
        truck=truck_urn,
        gap_seconds=time_gap,
        last_known_lat=position.get("lat"),
        last_known_lon=position.get("lon"),
    )

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


async def customs_hold_vector_stall(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    """Laredo Vector Stall detector: Fire alert when customs hold lands before driver reaches bridge.

    The "Vector Stall" problem: Customs broker knows about the hold hours before the driver
    does. If the driver is still >30 minutes from the bridge, we can reroute them to a
    hold lot instead of wasting hours in the queue.
    """
    if event.type not in (
        EventType.CUSTOMS_HOLD_LANDED.value,
        EventType.CUSTOMS_DOCUMENTATION_MISSING.value,
        EventType.CUSTOMS_INSPECTION_REQUIRED.value,
    ):
        return []

    data = event.data if isinstance(event.data, dict) else {}
    shipment_urn = event.subject or ""

    # Extract Laredo-specific context from Descartes normalization
    eta_to_bridge = data.get("eta_to_bridge_minutes")
    truck_location = data.get("truck_location", "unknown")
    port_of_entry = data.get("port_of_entry", "unknown")
    hold_reason = data.get("hold_reason", "unknown")

    # Only alert if driver hasn't reached bridge yet (>30 minutes away)
    if eta_to_bridge is None or eta_to_bridge <= 30:
        return []

    # Debounce per shipment to avoid duplicate alerts
    if not await _debounce(redis, f"vectorstall:{shipment_urn}", ttl=3600):
        return []

    log.info(
        "customs_hold.vector_stall",
        shipment=shipment_urn,
        port_of_entry=port_of_entry,
        eta_to_bridge_minutes=eta_to_bridge,
        truck_location=truck_location,
        hold_reason=hold_reason,
    )

    return [
        new_event(
            type=EventType.ALERT_CUSTOMS_HOLD_VECTOR_STALL,
            source="mandala/alerts",
            subject=shipment_urn,
            data={
                "shipment_urn": shipment_urn,
                "port_of_entry": port_of_entry,
                "hold_reason": hold_reason,
                "truck_location": truck_location,
                "eta_to_bridge_minutes": eta_to_bridge,
                "action_required": "reroute_driver_to_hold_lot",
                "message": (
                    f"🚨 CUSTOMS HOLD LANDED at {port_of_entry}. "
                    f"Driver is {truck_location}. ETA: {eta_to_bridge}min. "
                    f"DO NOT PROCEED TO BRIDGE. Reroute to hold lot."
                ),
                "severity": "critical",
            },
        )
    ]


DETECTORS = (cross_border, cold_chain, dead_zone, customs_hold_vector_stall)
