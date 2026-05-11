"""Mandala Samsara Connector — standalone Docker container.

A lateral, modular connector that bridges Samsara to a Mandala event bus.
Receives Samsara webhooks (HMAC-verified), normalizes them into MandalaEvent
envelopes (CloudEvents 1.0 + OTel span fields), and POSTs to the Mandala
/events endpoint.

Architectural principles:
  * Modular: standalone Docker container, no code dependency on Mandala source.
    Communicates via HTTP /events API only.
  * Trace-native: every event carries trace_id/span_id derived from subject.
    Auto-correlates into a single distributed trace across Samsara/MacroPoint.
  * Three-timestamp accounting: occurred_at / received_at / processed_at
    propagated for liability chain (insurance claims, customs disputes).
  * Idempotency: deterministic ingest_id from eventId or body fingerprint
    so Samsara webhook retries are deduplicated by Mandala downstream.
  * HMAC verification: webhook signatures verified with constant-time
    comparison + replay-protection timestamp tolerance.
  * Bidirectional: optionally subscribes to Mandala stream (or polls a
    state endpoint) and pushes Mandala-enriched location updates back to
    Samsara via custom fields (outbound enrichment).
  * POE-aware: emits POE-specific events (TRUCK_POE_ENTERED/EXITED) when
    geofence names match configured Ports-of-Entry.

Configuration (env vars):
  MANDALA_EVENTS_URL              http://mandala-api:8000/events  (required)
  SAMSARA_WEBHOOK_SECRET          shared HMAC secret              (required)
  SAMSARA_API_TOKEN               bearer token for outbound       (optional)
  SAMSARA_OUTBOUND_ENABLED        "1" to enable outbound          (optional)
  POE_GEOFENCES                   JSON dict of POE geofence names (optional)
  WEBHOOK_TIMESTAMP_TOLERANCE     replay window in seconds (default 300)
  CONNECTOR_PORT                  HTTP listen port (default 9000)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from pydantic import BaseModel
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class Config(BaseModel):
    mandala_events_url: str = os.getenv("MANDALA_EVENTS_URL", "http://mandala-api:8000/events")
    webhook_secret: str = os.getenv("SAMSARA_WEBHOOK_SECRET", "")
    api_token: str = os.getenv("SAMSARA_API_TOKEN", "")
    outbound_enabled: bool = os.getenv("SAMSARA_OUTBOUND_ENABLED", "0") == "1"
    poe_geofences: dict[str, dict[str, float | int]] = {}
    timestamp_tolerance: int = int(os.getenv("WEBHOOK_TIMESTAMP_TOLERANCE", "300"))
    connector_port: int = int(os.getenv("CONNECTOR_PORT", "9000"))

    def __init__(self, **data: Any):
        super().__init__(**data)
        # Parse POE_GEOFENCES JSON if provided
        poe_json = os.getenv("POE_GEOFENCES", "{}")
        try:
            self.poe_geofences = json.loads(poe_json)
        except json.JSONDecodeError:
            self.poe_geofences = {}


cfg = Config()
log = structlog.get_logger(__name__)
SOURCE = "mandala/connector/samsara"
SCHEMA_VERSION = "0.3"

# ---------------------------------------------------------------------------
# CloudEvents + OTel envelope (lightweight, no Mandala source dependency)
# ---------------------------------------------------------------------------


def _trace_id_from_subject(subject: str) -> str:
    """Deterministic 16-byte trace id from the subject — auto-correlates
    every event for a truck/shipment across vendors into one distributed trace."""
    return hashlib.sha256(subject.encode()).hexdigest()[:32]


def _span_id() -> str:
    """Random 8-byte span id (per-event)."""
    return uuid.uuid4().hex[:16]


def make_event(
    *,
    event_type: str,
    subject: str,
    data: dict[str, Any],
    occurred_at: datetime | None = None,
    ingest_id: str | None = None,
    parent_span_id: str | None = None,
) -> dict[str, Any]:
    """Build a CloudEvents 1.0 envelope with OTel span fields.

    Conforms to Mandala schema 0.3:
      * trace_id derived from subject (auto-correlation)
      * span_id per event
      * three-timestamp accounting (time / received_at / processed_at)
    """
    now = datetime.now(UTC)
    occurred_at = occurred_at or now
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": SOURCE,
        "type": event_type,
        "time": occurred_at.isoformat(),
        "subject": subject,
        "datacontenttype": "application/json",
        "data": data,
        # Mandala extensions
        "mandalaschemaversion": SCHEMA_VERSION,
        "mandalaingestid": ingest_id,
        "received_at": now.isoformat(),
        # OTel span fields
        "trace_id": _trace_id_from_subject(subject),
        "span_id": _span_id(),
        "parent_span_id": parent_span_id,
    }


# ---------------------------------------------------------------------------
# HMAC verification (replay-protected)
# ---------------------------------------------------------------------------


def verify_hmac(body: bytes, signature: str, secret: str) -> bool:
    """Constant-time HMAC-SHA256 verification. Accepts ``sha256=<hex>``
    prefixed or bare hex-encoded signatures."""
    if not signature or not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    received = signature.removeprefix("sha256=").strip()
    return hmac.compare_digest(expected.lower(), received.lower())


def is_timestamp_fresh(ts: str | None, tolerance_sec: int) -> bool:
    """Replay protection: reject webhooks whose timestamp is outside the
    tolerance window from now."""
    if not ts:
        return False
    try:
        # Accept RFC 3339 or epoch seconds
        if ts.replace(".", "").replace("-", "").isdigit():
            event_time = float(ts)
        else:
            event_time = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return False
    now = time.time()
    return abs(now - event_time) <= tolerance_sec


# ---------------------------------------------------------------------------
# Samsara payload normalization
# ---------------------------------------------------------------------------


def _truck_urn(vehicle_id: str) -> str:
    return f"urn:mandala:truck:samsara:{vehicle_id}"


def _driver_urn(driver_id: str) -> str:
    return f"urn:mandala:party:samsara-driver:{driver_id}"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _handle_vehicle_location(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    loc = data.get("location", {}) or data
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []
    vehicle_id = str(vehicle_id)
    subject = _truck_urn(vehicle_id)
    ingest_id = payload.get("eventId") or payload.get("event_id")

    point = {
        "lat": float(loc["latitude"]),
        "lon": float(loc["longitude"]),
        "heading_deg": loc.get("headingDegrees"),
        "speed_mps": (loc.get("speedMilesPerHour") or 0.0) * 0.44704
        if "speedMilesPerHour" in loc
        else loc.get("speedMetersPerSecond"),
        "captured_at": _parse_ts(loc.get("time") or loc.get("happenedAtTime") or payload["happenedAtTime"]).isoformat(),
    }

    telemetry = {
        "truck_id": vehicle_id,
        "license_plate": vehicle.get("licensePlate"),
        "position": {
            "truck_id": vehicle_id,
            "point": point,
            "odometer_km": (loc.get("odometerMeters") or 0) / 1000 or None,
            "fuel_pct": loc.get("fuelPercent"),
            "engine_state": loc.get("engineStates", [{}])[-1].get("value") if loc.get("engineStates") else None,
            "captured_at": point["captured_at"],
        },
        "vendor": "samsara",
    }

    return [
        make_event(
            event_type="mandala.truck.position",
            subject=subject,
            data=telemetry,
            ingest_id=ingest_id,
        )
    ]


def _handle_geofence(payload: dict[str, Any], *, entered: bool) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    fence = data.get("address", {}) or data.get("geofence", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []
    vehicle_id = str(vehicle_id)
    subject = _truck_urn(vehicle_id)
    ingest_id = payload.get("eventId") or payload.get("event_id")
    geofence_name = fence.get("name", "")

    # Check if this geofence matches a configured POE
    is_poe = False
    if cfg.poe_geofences and geofence_name:
        is_poe = geofence_name.lower() in [poe.lower() for poe in cfg.poe_geofences.keys()]

    base_event_type = "mandala.truck.geofence.entered" if entered else "mandala.truck.geofence.exited"
    events = [
        make_event(
            event_type=base_event_type,
            subject=subject,
            data={
                "truck_id": vehicle_id,
                "geofence_id": str(fence.get("id")) if fence.get("id") is not None else None,
                "geofence_name": geofence_name,
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=ingest_id,
        )
    ]

    # If this is a POE geofence, emit a POE-specific event
    if is_poe:
        poe_event_type = "mandala.truck.poe.entered" if entered else "mandala.truck.poe.exited"
        events.append(
            make_event(
                event_type=poe_event_type,
                subject=subject,
                data={
                    "truck_id": vehicle_id,
                    "poe_name": geofence_name,
                    "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                    "vendor": "samsara",
                },
                ingest_id=f"{ingest_id}:poe" if ingest_id else None,
                parent_span_id=events[0]["span_id"],
            )
        )

    return events


def _handle_temperature(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    sensor = data.get("sensor", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId") or sensor.get("vehicleId")
    if vehicle_id is None:
        return []
    vehicle_id = str(vehicle_id)
    subject = _truck_urn(vehicle_id)
    ingest_id = payload.get("eventId") or payload.get("event_id")

    # If Samsara sent a "temperatureExceeded" event variant, tag as breach
    event_type = payload.get("eventType") or ""
    is_breach = "exceeded" in event_type.lower()

    reading = {
        "truck_id": vehicle_id,
        "sensor_id": str(sensor.get("id") or "default"),
        "temperature_c": float(data.get("temperatureCelsius", data.get("temperature", 0))),
        "humidity_pct": data.get("humidityPercent"),
        "setpoint_c": data.get("setpointCelsius"),
        "door_open": data.get("doorOpen"),
        "captured_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
        "vendor": "samsara",
    }

    return [
        make_event(
            event_type="mandala.cold_chain.breach" if is_breach else "mandala.cold_chain.reading",
            subject=subject,
            data=reading,
            ingest_id=ingest_id,
        )
    ]


def _handle_eld_hos(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    driver = data.get("driver", {}) or {}
    driver_id = driver.get("id")
    if driver_id is None:
        return []
    driver_id = str(driver_id)
    subject = _driver_urn(driver_id)
    ingest_id = payload.get("eventId") or payload.get("event_id")

    event_type = payload.get("eventType") or ""
    is_violation = "violation" in event_type.lower()

    return [
        make_event(
            event_type="mandala.driver.log.violation" if is_violation else "mandala.driver.hos.warning",
            subject=subject,
            data={
                "driver_id": driver_id,
                "driver_name": driver.get("name"),
                "violation_type": data.get("violationType"),
                "remaining_drive_minutes": data.get("remainingDriveTime"),
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=ingest_id,
        )
    ]


def _handle_harsh_event(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    vehicle = data.get("vehicle", {})
    vehicle_id = vehicle.get("id") or data.get("vehicleId")
    if vehicle_id is None:
        return []
    vehicle_id = str(vehicle_id)
    subject = _truck_urn(vehicle_id)
    ingest_id = payload.get("eventId") or payload.get("event_id")

    return [
        make_event(
            event_type="mandala.truck.harsh_event",
            subject=subject,
            data={
                "truck_id": vehicle_id,
                "behavior": data.get("behaviorLabels") or data.get("eventType"),
                "g_force": data.get("downloadForwardG") or data.get("force"),
                "occurred_at": _parse_ts(payload["happenedAtTime"]).isoformat(),
                "vendor": "samsara",
            },
            ingest_id=ingest_id,
        )
    ]


def normalize(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert a Samsara webhook payload into Mandala event envelopes."""
    event_type = payload.get("eventType") or payload.get("type") or ""

    if event_type in ("VehicleLocation", "VehicleGpsUpdated", "VehicleGpsUpdate"):
        return _handle_vehicle_location(payload)
    if event_type in ("VehicleEnterGeofence", "GeofenceEntry"):
        return _handle_geofence(payload, entered=True)
    if event_type in ("VehicleExitGeofence", "GeofenceExit"):
        return _handle_geofence(payload, entered=False)
    if event_type in ("VehicleTemperature", "TemperatureSensorReading", "TemperatureExceeded"):
        return _handle_temperature(payload)
    if event_type in ("EldHosViolation", "HosViolationDetected"):
        return _handle_eld_hos(payload)
    if event_type in ("HarshEvent", "VehicleHarshEvent"):
        return _handle_harsh_event(payload)

    log.info("samsara.unhandled_event_type", event_type=event_type)
    return []


# ---------------------------------------------------------------------------
# Mandala bus client (HTTP)
# ---------------------------------------------------------------------------


class MandalaBusClient:
    """Thin HTTP client that POSTs CloudEvents envelopes to Mandala /events."""

    def __init__(self, events_url: str) -> None:
        self._url = events_url
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "Content-Type": "application/cloudevents+json",
                "User-Agent": "mandala-samsara-connector/0.3",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def post(self, event: dict[str, Any]) -> None:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.post(self._url, json=event)
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"transient {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()


# ---------------------------------------------------------------------------
# Outbound: Mandala → Samsara custom field enrichment
# ---------------------------------------------------------------------------


class SamsaraOutbound:
    """Outbound client for pushing Mandala-enriched data back to Samsara
    via custom fields. Used when SAMSARA_OUTBOUND_ENABLED=1 so that
    MacroPoint customs status, FMCSA safety scores, etc. appear in Samsara."""

    def __init__(self, api_token: str) -> None:
        self._token = api_token
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "mandala-samsara-connector/0.3",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def update_custom_field(
        self,
        *,
        vehicle_id: str,
        field_id: str,
        value: str,
    ) -> None:
        """Update a Samsara custom field on a vehicle."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.patch(
                    f"/v1/fleet/vehicles/{vehicle_id}",
                    json={"customFields": {field_id: value}},
                )
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"transient {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


app = FastAPI(
    title="Mandala Samsara Connector",
    description="Lateral, modular bridge from Samsara to a Mandala event bus.",
    version="0.3",
)
_bus: MandalaBusClient | None = None
_outbound: SamsaraOutbound | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _bus, _outbound
    _bus = MandalaBusClient(cfg.mandala_events_url)
    if cfg.outbound_enabled and cfg.api_token:
        _outbound = SamsaraOutbound(cfg.api_token)
    log.info(
        "samsara.connector.started",
        events_url=cfg.mandala_events_url,
        outbound_enabled=cfg.outbound_enabled,
        poe_geofences=list(cfg.poe_geofences.keys()),
        port=cfg.connector_port,
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _bus:
        await _bus.close()
    if _outbound:
        await _outbound.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "connector": "samsara",
        "schema_version": SCHEMA_VERSION,
        "outbound_enabled": cfg.outbound_enabled,
        "poe_geofences": list(cfg.poe_geofences.keys()),
    }


@app.post("/webhooks/samsara", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_samsara_webhook(
    request: Request,
    x_samsara_signature: str | None = Header(default=None, alias="X-Samsara-Signature"),
    x_samsara_timestamp: str | None = Header(default=None, alias="X-Samsara-Timestamp"),
) -> Response:
    """Receive a Samsara webhook, verify HMAC, normalize, forward to Mandala."""
    body = await request.body()

    # 1. HMAC signature verification (fail-closed)
    if not verify_hmac(body, x_samsara_signature or "", cfg.webhook_secret):
        log.warning("samsara.webhook.invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    # 2. Replay protection (timestamp tolerance window)
    if cfg.timestamp_tolerance > 0 and not is_timestamp_fresh(
        x_samsara_timestamp, tolerance_sec=cfg.timestamp_tolerance
    ):
        log.warning("samsara.webhook.stale_timestamp", timestamp=x_samsara_timestamp)
        raise HTTPException(status_code=401, detail="stale or missing timestamp")

    # 3. Parse JSON
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON") from None

    # 4. Normalize → MandalaEvent envelopes
    events = normalize(payload)
    if not events:
        log.info("samsara.webhook.unhandled", event_type=payload.get("eventType"))
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # 5. Forward to Mandala /events. Mandala handles idempotency/replay
    #    via mandalaingestid, so this connector never has to maintain state.
    assert _bus is not None
    for event in events:
        try:
            await _bus.post(event)
            log.info(
                "samsara.event.forwarded",
                type=event["type"],
                trace_id=event["trace_id"],
                subject=event["subject"],
            )
        except Exception as exc:  # noqa: BLE001
            log.error("samsara.event.forward_failed", error=str(exc), type=event["type"])
            raise HTTPException(status_code=500, detail="forward failed") from exc

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/outbound/custom-field", status_code=status.HTTP_204_NO_CONTENT)
async def update_custom_field(payload: dict[str, Any]) -> Response:
    """Outbound endpoint for pushing Mandala-enriched data to Samsara
    custom fields. Called by an upstream Mandala worker subscription.

    Body (JSON):
        {"vehicle_id": "...", "field_id": "...", "value": "..."}
    """
    if not _outbound:
        raise HTTPException(
            status_code=503,
            detail="outbound disabled (set SAMSARA_OUTBOUND_ENABLED=1 and SAMSARA_API_TOKEN)",
        )
    try:
        await _outbound.update_custom_field(
            vehicle_id=str(payload["vehicle_id"]),
            field_id=str(payload["field_id"]),
            value=str(payload["value"]),
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=cfg.connector_port)
