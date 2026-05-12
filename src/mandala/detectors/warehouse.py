"""Warehouse operations detectors.

Multi-sensor correlation for predictive dock synchronization and verified empty
mile detection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.state import StateStore

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Predictive Dock Synchronization
# ---------------------------------------------------------------------------


async def detect_dock_readiness(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    """Correlate truck position + customs status + HOS to predict dock readiness.

    This detector solves the "Predictive Dock Synchronization" problem by:
    1. Checking if the linked shipment has a customs hold
    2. Calculating real-time ETA based on current position
    3. Checking HOS remaining from Samsara telemetry
    4. Emitting dock readiness events that WMS can consume

    Emits:
    - mandala.shipment.dock.blocked when physical constraints prevent arrival
    - mandala.shipment.dock.ready when all constraints are satisfied
    """
    if event.type != EventType.TRUCK_POSITION.value:
        return []

    truck_urn = event.subject or ""
    if not truck_urn.startswith("urn:mandala:truck:"):
        return []

    # Get linked shipment
    shipment_urn = await state.shipment_for_truck(truck_urn)
    if not shipment_urn:
        return []

    # Get shipment state (includes customs status)
    shipment = await state.get("shipment", shipment_urn) or {}
    customs_status = shipment.get("customs_status")

    # Physical constraint: customs hold
    if customs_status in ("hold", "exam"):
        log.info(
            "dock.blocked.customs_hold",
            truck=truck_urn,
            shipment=shipment_urn,
            customs_status=customs_status,
        )
        return [
            new_event(
                type=EventType.SHIPMENT_DOCK_BLOCKED,
                source="mandala/detector/dock_readiness",
                subject=shipment_urn,
                data={
                    "truck_urn": truck_urn,
                    "shipment_urn": shipment_urn,
                    "reason": "customs_hold",
                    "customs_status": customs_status,
                    "blocked_at": datetime.now(UTC).isoformat(),
                },
            )
        ]

    # Get truck state (includes HOS, position)
    truck = await state.get("truck", truck_urn) or {}
    pos = truck.get("last_position") or {}
    hos_remaining_min = truck.get("hos_remaining_min")

    # Physical constraint: HOS limit
    if hos_remaining_min is not None and hos_remaining_min < 60:
        log.info(
            "dock.blocked.hos_limit",
            truck=truck_urn,
            shipment=shipment_urn,
            hos_remaining_min=hos_remaining_min,
        )
        return [
            new_event(
                type=EventType.SHIPMENT_DOCK_BLOCKED,
                source="mandala/detector/dock_readiness",
                subject=shipment_urn,
                data={
                    "truck_urn": truck_urn,
                    "shipment_urn": shipment_urn,
                    "reason": "hos_limit",
                    "hos_remaining_min": hos_remaining_min,
                    "blocked_at": datetime.now(UTC).isoformat(),
                },
            )
        ]

    # Calculate real-time ETA based on position (simplified)
    # In production, this would call a traffic API (ATRI, Google Maps, etc.)
    lat = pos.get("lat")
    lon = pos.get("lon")
    destination = shipment.get("destination_address")

    if lat and lon and destination:
        # TODO: Integrate with traffic API for accurate ETA
        # For now, emit readiness event with current position
        log.info(
            "dock.ready",
            truck=truck_urn,
            shipment=shipment_urn,
            position={"lat": lat, "lon": lon},
        )
        return [
            new_event(
                type=EventType.SHIPMENT_DOCK_READY,
                source="mandala/detector/dock_readiness",
                subject=shipment_urn,
                data={
                    "truck_urn": truck_urn,
                    "shipment_urn": shipment_urn,
                    "current_position": {"lat": lat, "lon": lon},
                    "destination": destination,
                    "customs_status": customs_status,
                    "hos_remaining_min": hos_remaining_min,
                    "ready_at": datetime.now(UTC).isoformat(),
                },
            )
        ]

    return []


# ---------------------------------------------------------------------------
# High-Fidelity Empty Mile Broadcast
# ---------------------------------------------------------------------------


def _is_receiver_geofence(geofence_name: str | None) -> bool:
    """Check if a geofence is a receiver/unloading location.

    In production, this would be configurable via environment variable:
    MANDALA_RECEIVER_GEOFENCES='["DC-Chicago", "Warehouse-Dallas", ...]'
    """
    if not geofence_name:
        return False

    # Simplified heuristic: receiver geofences typically contain these keywords
    receiver_keywords = ("dc", "warehouse", "receiver", "unloading", "distribution")
    geofence_lower = geofence_name.lower()
    return any(keyword in geofence_lower for keyword in receiver_keywords)


async def detect_verified_empty(event: MandalaEvent, state: StateStore, redis: object) -> list[MandalaEvent]:
    """Multi-sensor verification: cargo_sensor=0% + geofence_exit = verified empty.

    This detector solves the "High-Fidelity Empty Mile Broadcast" problem by:
    1. Detecting when a truck exits a receiver geofence
    2. Checking cargo sensor percentage (if available from Samsara)
    3. Emitting a verified empty event with higher confidence than delivery-based

    Emits:
    - mandala.truck.empty.verified when multi-sensor verification passes
    """
    if event.type != EventType.TRUCK_GEOFENCE_EXITED.value:
        return []

    data = event.data if isinstance(event.data, dict) else {}
    geofence_name = data.get("geofence_name") or data.get("geofence_id")

    # Check if this is a receiver location
    if not _is_receiver_geofence(geofence_name):
        return []

    truck_urn = event.subject or ""
    if not truck_urn.startswith("urn:mandala:truck:"):
        return []

    # Get truck state (includes cargo sensor if available)
    truck = await state.get("truck", truck_urn) or {}
    cargo_sensor_pct = truck.get("cargo_sensor_percent")

    # Multi-sensor verification: cargo sensor should be near 0%
    if cargo_sensor_pct is not None and cargo_sensor_pct > 5:
        log.info(
            "empty.verification.failed.cargo_not_empty",
            truck=truck_urn,
            cargo_pct=cargo_sensor_pct,
            geofence=geofence_name,
        )
        return []

    log.info(
        "empty.verified",
        truck=truck_urn,
        cargo_pct=cargo_sensor_pct,
        geofence=geofence_name,
        verification_method="multi_sensor" if cargo_sensor_pct is not None else "geofence_only",
    )

    # Emit verified empty event (higher confidence than delivery-based)
    return [
        new_event(
            type=EventType.TRUCK_EMPTY_VERIFIED,
            source="mandala/detector/verified_empty",
            subject=truck_urn,
            data={
                "truck_urn": truck_urn,
                "verified_at": event.time.astimezone(UTC).isoformat(),
                "verification_method": "multi_sensor" if cargo_sensor_pct is not None else "geofence_only",
                "cargo_sensor_pct": cargo_sensor_pct,
                "geofence_exited": geofence_name,
                "last_position": truck.get("last_position"),
                "equipment": truck.get("equipment"),
                "vin": truck.get("vin"),
                "license_plate": truck.get("license_plate"),
            },
        )
    ]


DETECTORS = (detect_dock_readiness, detect_verified_empty)
