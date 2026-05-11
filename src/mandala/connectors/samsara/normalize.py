"""Normalize Samsara webhook payloads into :class:`MandalaEvent` objects.

Reference: Samsara Webhooks documentation
https://developers.samsara.com/docs/webhooks

Samsara delivers each webhook as a JSON document with a top-level
``eventType`` discriminator and a ``data`` object whose shape depends on
the type. This module is intentionally a pure function table — easy to
unit-test, no I/O, no config.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.schema.geo import GeoPoint
from mandala.core.schema.identifiers import URN
from mandala.core.schema.truck import (
    ColdChainReading,
    Truck,
    TruckPosition,
    TruckTelemetry,
)

SOURCE = "mandala/connector/samsara"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _truck_urn(vehicle_id: Any) -> str:
    return str(URN.truck(scope="samsara", id=str(vehicle_id)))


def _ingest_id(payload: dict[str, Any]) -> str | None:
    # Samsara webhook payloads carry an ``eventId`` (UUID) per delivery.
    return payload.get("eventId") or payload.get("event_id")


# --- Per-eventType handlers ----------------------------------------------


def _handle_vehicle_location(payload: dict[str, Any]) -> list[MandalaEvent]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    loc = data.get("location", {}) or data
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []

    point = GeoPoint(
        lat=float(loc["latitude"]),
        lon=float(loc["longitude"]),
        heading_deg=loc.get("headingDegrees"),
        speed_mps=(loc.get("speedMilesPerHour") or 0.0) * 0.44704
        if "speedMilesPerHour" in loc
        else loc.get("speedMetersPerSecond"),
        captured_at=_parse_ts(loc.get("time") or loc.get("happenedAtTime") or payload["happenedAtTime"]),
    )
    telemetry = TruckTelemetry(
        truck=Truck(id=str(vehicle_id), license_plate=vehicle.get("licensePlate")),
        position=TruckPosition(
            truck_id=str(vehicle_id),
            point=point,
            odometer_km=(loc.get("odometerMeters") or 0) / 1000 or None,
            fuel_pct=loc.get("fuelPercent"),
            engine_state=loc.get("engineStates", [{}])[-1].get("value") if loc.get("engineStates") else None,
            captured_at=point.captured_at or _parse_ts(payload["happenedAtTime"]),
        ),
    )
    return [
        new_event(
            type=EventType.TRUCK_POSITION,
            source=SOURCE,
            subject=_truck_urn(vehicle_id),
            data=telemetry,
            ingest_id=_ingest_id(payload),
        )
    ]


def _handle_geofence(payload: dict[str, Any], *, entered: bool, poe_geofences: dict[str, dict[str, float | int]] | None = None) -> list[MandalaEvent]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    fence = data.get("address", {}) or data.get("geofence", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []
    
    geofence_name = fence.get("name", "")
    
    # Check if this geofence matches a configured POE
    is_poe = False
    if poe_geofences and geofence_name:
        is_poe = geofence_name.lower() in [poe.lower() for poe in poe_geofences.keys()]
    
    events = [
        new_event(
            type=EventType.TRUCK_GEOFENCE_ENTERED if entered else EventType.TRUCK_GEOFENCE_EXITED,
            source=SOURCE,
            subject=_truck_urn(vehicle_id),
            data={
                "truck_id": str(vehicle_id),
                "geofence_id": str(fence.get("id")) if fence.get("id") is not None else None,
                "geofence_name": geofence_name,
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=_ingest_id(payload),
        )
    ]
    
    # If this is a POE geofence, emit a POE-specific event
    if is_poe:
        events.append(
            new_event(
                type=EventType.TRUCK_POE_ENTERED if entered else EventType.TRUCK_POE_EXITED,
                source=SOURCE,
                subject=_truck_urn(vehicle_id),
                data={
                    "truck_id": str(vehicle_id),
                    "poe_name": geofence_name,
                    "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                    "vendor": "samsara",
                },
                ingest_id=f"{_ingest_id(payload)}:poe" if _ingest_id(payload) else None,
            )
        )
    
    return events


def _handle_temperature(payload: dict[str, Any]) -> list[MandalaEvent]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    sensor = data.get("sensor", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId") or sensor.get("vehicleId")
    if vehicle_id is None:
        return []

    reading = ColdChainReading(
        truck_id=str(vehicle_id),
        sensor_id=str(sensor.get("id") or "default"),
        temperature_c=float(data.get("temperatureCelsius", data.get("temperature", 0))),
        humidity_pct=data.get("humidityPercent"),
        setpoint_c=data.get("setpointCelsius"),
        door_open=data.get("doorOpen"),
        captured_at=_parse_ts(payload["happenedAtTime"]),
    )

    # If Samsara sent a "temperatureExceeded" event variant, tag as breach.
    et = (
        EventType.COLD_CHAIN_BREACH
        if (payload.get("eventType") or "").lower().endswith("exceeded")
        else EventType.COLD_CHAIN_READING
    )
    return [
        new_event(
            type=et,
            source=SOURCE,
            subject=_truck_urn(vehicle_id),
            data=reading,
            ingest_id=_ingest_id(payload),
        )
    ]


def _handle_eld_hos(payload: dict[str, Any]) -> list[MandalaEvent]:
    data = payload.get("data", {})
    driver = data.get("driver", {}) or {}
    driver_id = driver.get("id")
    if driver_id is None:
        return []
    et = (
        EventType.DRIVER_LOG_VIOLATION
        if "violation" in (payload.get("eventType") or "").lower()
        else EventType.DRIVER_HOS_WARNING
    )
    return [
        new_event(
            type=et,
            source=SOURCE,
            subject=str(URN.party(scope="samsara-driver", id=str(driver_id))),
            data={
                "driver_id": str(driver_id),
                "driver_name": driver.get("name"),
                "violation_type": data.get("violationType"),
                "remaining_drive_minutes": data.get("remainingDriveTime"),
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=_ingest_id(payload),
        )
    ]


def _handle_harsh_event(payload: dict[str, Any]) -> list[MandalaEvent]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []
    return [
        new_event(
            type=EventType.TRUCK_HARSH_EVENT,
            source=SOURCE,
            subject=_truck_urn(vehicle_id),
            data={
                "truck_id": str(vehicle_id),
                "behavior": data.get("behaviorLabels") or data.get("eventType"),
                "g_force": data.get("downloadForwardG") or data.get("force"),
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=_ingest_id(payload),
        )
    ]


_HANDLERS: dict[str, Callable[[dict[str, Any]], list[MandalaEvent]]] = {
    "VehicleLocation": _handle_vehicle_location,
    "VehicleGpsUpdated": _handle_vehicle_location,
    "VehicleGpsUpdate": _handle_vehicle_location,
    "VehicleEnterGeofence": lambda p: _handle_geofence(p, entered=True),
    "VehicleExitGeofence": lambda p: _handle_geofence(p, entered=False),
    "GeofenceEntry": lambda p: _handle_geofence(p, entered=True),
    "GeofenceExit": lambda p: _handle_geofence(p, entered=False),
    "VehicleTemperature": _handle_temperature,
    "TemperatureSensorReading": _handle_temperature,
    "TemperatureExceeded": _handle_temperature,
    "EldHosViolation": _handle_eld_hos,
    "HosViolationDetected": _handle_eld_hos,
    "HarshEvent": _handle_harsh_event,
    "VehicleHarshEvent": _handle_harsh_event,
}


def normalize(payload: dict[str, Any], *, poe_geofences: dict[str, dict[str, float | int]] | None = None) -> list[MandalaEvent]:
    """Convert a Samsara webhook payload into zero or more :class:`MandalaEvent` objects.
    
    Args:
        payload: The Samsara webhook payload
        poe_geofences: Optional dict of configured POE geofences for POE-specific event emission
    """
    event_type = payload.get("eventType") or payload.get("type") or ""
    handler = _HANDLERS.get(event_type)
    if handler is None:
        return []
    try:
        # Pass poe_geofences to geofence handlers
        if "geofence" in event_type.lower():
            return _handle_geofence(payload, entered="enter" in event_type.lower(), poe_geofences=poe_geofences)
        return handler(payload)
    except (KeyError, ValueError, TypeError):
        # Malformed payload — caller decides whether to DLQ or drop.
        return []
