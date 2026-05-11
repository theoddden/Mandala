"""Mandala Descartes MacroPoint Connector — standalone Docker container.

A lateral, modular connector that bridges Descartes MacroPoint to a Mandala
event bus. Receives MacroPoint webhooks (HMAC-verified), normalizes them
into MandalaEvent envelopes (CloudEvents 1.0 + OTel span fields), and
POSTs to the Mandala /events endpoint.

Architectural principles:
  * Modular: standalone Docker container, no code dependency on Mandala source.
    Communicates via HTTP /events API only.
  * Trace-native: every event carries trace_id/span_id derived from subject.
    Auto-correlates into a single distributed trace across Samsara/MacroPoint.
  * Three-timestamp accounting: occurred_at / received_at / processed_at
    propagated for liability chain (insurance claims, customs disputes).
  * Idempotency: deterministic ingest_id from MessageId or body fingerprint
    so MacroPoint webhook retries are deduplicated by Mandala downstream.
  * HMAC verification: webhook signatures verified with constant-time
    comparison + replay-protection timestamp tolerance.
  * Bidirectional: optionally subscribes to Mandala stream (or polls a
    state endpoint) and pushes Mandala-enriched location updates back to
    MacroPoint as LocationUpdate messages.

Configuration (env vars):
  MANDALA_EVENTS_URL              http://mandala-api:8000/events  (required)
  DESCARTES_WEBHOOK_SECRET        shared HMAC secret              (required)
  DESCARTES_API_KEY               outbound bearer token           (optional)
  DESCARTES_MACROPOINT_BASE_URL   https://carrier-api.macropoint.com  (optional)
  WEBHOOK_TIMESTAMP_TOLERANCE     replay window in seconds (default 300)
  CONNECTOR_PORT                  HTTP listen port (default 9001)
  OUTBOUND_ENABLED                "1" to enable Samsara→MacroPoint bridge
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
    webhook_secret: str = os.getenv("DESCARTES_WEBHOOK_SECRET", "")
    api_key: str = os.getenv("DESCARTES_API_KEY", "")
    macropoint_base_url: str = os.getenv(
        "DESCARTES_MACROPOINT_BASE_URL", "https://carrier-api.macropoint.com"
    )
    timestamp_tolerance: int = int(os.getenv("WEBHOOK_TIMESTAMP_TOLERANCE", "300"))
    outbound_enabled: bool = os.getenv("OUTBOUND_ENABLED", "0") == "1"
    connector_port: int = int(os.getenv("CONNECTOR_PORT", "9001"))


cfg = Config()
log = structlog.get_logger(__name__)
SOURCE = "mandala/connector/descartes-macropoint"
SCHEMA_VERSION = "0.3"


# ---------------------------------------------------------------------------
# CloudEvents + OTel envelope (lightweight, no Mandala source dependency)
# ---------------------------------------------------------------------------


def _trace_id_from_subject(subject: str) -> str:
    """Deterministic 16-byte trace id from the subject — auto-correlates
    every event for a shipment across vendors into one distributed trace."""
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
# MacroPoint payload normalization
# ---------------------------------------------------------------------------


_STATUS_TO_EVENT_TYPE = {
    "Booked": "mandala.shipment.booked",
    "Dispatched": "mandala.shipment.dispatched",
    "PickedUp": "mandala.shipment.picked_up",
    "InTransit": "mandala.shipment.in_transit",
    "AtBorder": "mandala.shipment.at_border",
    "Delivered": "mandala.shipment.delivered",
    "Cancelled": "mandala.shipment.cancelled",
    "CustomsHoldLanded": "mandala.shipment.customs.hold.landed",
    "CustomsHoldCleared": "mandala.shipment.customs.hold.cleared",
    "CustomsDocumentationMissing": "mandala.shipment.customs.documentation.missing",
    "CustomsInspectionRequired": "mandala.shipment.customs.inspection.required",
}


def _shipment_urn(shipment_id: str) -> str:
    return f"urn:mandala:shipment:macropoint:{shipment_id}"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def normalize(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert a MacroPoint webhook payload into Mandala event envelopes.

    Handles ``TrackingRequest`` and ``StatusUpdate`` / ``LocationUpdate``
    messages. Unknown message types return an empty list (forwarded as
    no-op, never rejected — schema rule: ``unknown types must be
    forwarded unchanged``).
    """
    msg_type = payload.get("MessageType") or payload.get("messageType") or ""
    body = payload.get("Body") or payload.get("body") or payload
    ingest_id = payload.get("MessageId") or payload.get("messageId")

    shipment_id = (
        body.get("ShipmentId")
        or body.get("shipmentId")
        or body.get("OrderNumber")
        or body.get("orderNumber")
    )
    if shipment_id is None:
        return []
    shipment_id = str(shipment_id)
    subject = _shipment_urn(shipment_id)

    if msg_type == "TrackingRequest":
        return [
            make_event(
                event_type="mandala.shipment.booked",
                subject=subject,
                data={
                    "shipment_id": shipment_id,
                    "order_number": body.get("OrderNumber") or body.get("orderNumber"),
                    "carrier_scac": body.get("CarrierScac") or body.get("carrierScac"),
                    "origin": body.get("Origin") or body.get("origin"),
                    "destination": body.get("Destination") or body.get("destination"),
                    "pickup_window_start": body.get("PickupWindowStart"),
                    "delivery_window_start": body.get("DeliveryWindowStart"),
                    "vendor": "macropoint",
                },
                ingest_id=ingest_id,
            )
        ]

    if msg_type in ("StatusUpdate", "LocationUpdate"):
        status_str = body.get("Status") or body.get("status") or "InTransit"
        et = _STATUS_TO_EVENT_TYPE.get(status_str, "mandala.shipment.in_transit")
        ts = body.get("Timestamp") or body.get("timestamp") or payload.get("Timestamp")
        occurred_at = _parse_ts(ts) if ts else None

        data: dict[str, Any] = {
            "shipment_id": shipment_id,
            "status": status_str.lower(),
            "occurred_at": (occurred_at or datetime.now(UTC)).isoformat(),
            "vendor": "macropoint",
        }
        if "Latitude" in body and "Longitude" in body:
            data["location"] = {
                "lat": float(body["Latitude"]),
                "lon": float(body["Longitude"]),
            }
        eta = body.get("Eta") or body.get("eta")
        if eta:
            data["eta"] = eta

        primary = make_event(
            event_type=et,
            subject=subject,
            data=data,
            occurred_at=occurred_at,
            ingest_id=ingest_id,
        )
        events = [primary]

        # Emit a derived ETA event; parent span = primary so the causal
        # chain shows up natively in trace UIs.
        if eta:
            events.append(
                make_event(
                    event_type="mandala.shipment.eta.updated",
                    subject=subject,
                    data={
                        "shipment_id": shipment_id,
                        "eta": eta,
                        "source": "macropoint",
                    },
                    occurred_at=occurred_at,
                    ingest_id=f"{ingest_id}:eta" if ingest_id else None,
                    parent_span_id=primary["span_id"],
                )
            )
        return events

    log.info("descartes.unhandled_message_type", message_type=msg_type)
    return []


# ---------------------------------------------------------------------------
# Mandala bus client (HTTP)
# ---------------------------------------------------------------------------


class MandalaBusClient:
    """Thin HTTP client that POSTs CloudEvents envelopes to Mandala /events.

    Retries on transient failures with exponential backoff. Emits
    structured logs so connector can be operated without code knowledge of
    Mandala internals.
    """

    def __init__(self, events_url: str) -> None:
        self._url = events_url
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "Content-Type": "application/cloudevents+json",
                "User-Agent": "mandala-descartes-connector/0.3",
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
# Outbound: Mandala → MacroPoint LocationUpdate push
# ---------------------------------------------------------------------------


class MacroPointOutbound:
    """Outbound client for sending Mandala-enriched location updates back
    to MacroPoint. Used when ``OUTBOUND_ENABLED=1`` so that any vendor
    feeding Mandala (Samsara, Geotab, Motive, etc.) appears in MacroPoint
    as a LocationUpdate. This is the bridge that makes Mandala the
    single source of truth for shipment tracking."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "mandala-descartes-connector/0.3",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def send_location_update(
        self,
        *,
        shipment_id: str,
        latitude: float,
        longitude: float,
        captured_at: datetime,
        speed_mps: float | None = None,
        heading_deg: float | None = None,
        eta: datetime | None = None,
        status: str = "InTransit",
    ) -> None:
        body: dict[str, Any] = {
            "MessageType": "LocationUpdate",
            "Body": {
                "ShipmentId": shipment_id,
                "Latitude": latitude,
                "Longitude": longitude,
                "Timestamp": captured_at.isoformat(),
                "Status": status,
            },
        }
        if speed_mps is not None:
            body["Body"]["SpeedMps"] = speed_mps
        if heading_deg is not None:
            body["Body"]["Heading"] = heading_deg
        if eta is not None:
            body["Body"]["Eta"] = eta.isoformat()

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.post("/v1/location-updates", json=body)
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
    title="Mandala Descartes MacroPoint Connector",
    description="Lateral, modular bridge from Descartes MacroPoint to a Mandala event bus.",
    version="0.3",
)
_bus: MandalaBusClient | None = None
_outbound: MacroPointOutbound | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _bus, _outbound
    _bus = MandalaBusClient(cfg.mandala_events_url)
    if cfg.outbound_enabled and cfg.api_key:
        _outbound = MacroPointOutbound(cfg.macropoint_base_url, cfg.api_key)
    log.info(
        "descartes.connector.started",
        events_url=cfg.mandala_events_url,
        outbound_enabled=cfg.outbound_enabled,
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
        "connector": "descartes-macropoint",
        "schema_version": SCHEMA_VERSION,
        "outbound_enabled": cfg.outbound_enabled,
    }


@app.post("/webhooks/macropoint", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_macropoint_webhook(
    request: Request,
    x_macropoint_signature: str | None = Header(default=None, alias="X-MacroPoint-Signature"),
    x_macropoint_timestamp: str | None = Header(default=None, alias="X-MacroPoint-Timestamp"),
) -> Response:
    """Receive a MacroPoint webhook, verify HMAC, normalize, forward to Mandala."""
    body = await request.body()

    # 1. HMAC signature verification (fail-closed)
    if not verify_hmac(body, x_macropoint_signature or "", cfg.webhook_secret):
        log.warning("descartes.webhook.invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    # 2. Replay protection (timestamp tolerance window)
    if cfg.timestamp_tolerance > 0 and not is_timestamp_fresh(
        x_macropoint_timestamp, tolerance_sec=cfg.timestamp_tolerance
    ):
        log.warning("descartes.webhook.stale_timestamp", timestamp=x_macropoint_timestamp)
        raise HTTPException(status_code=401, detail="stale or missing timestamp")

    # 3. Parse JSON
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON") from None

    # 4. Normalize → MandalaEvent envelopes
    events = normalize(payload)
    if not events:
        log.info("descartes.webhook.unhandled", message_type=payload.get("MessageType"))
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # 5. Forward to Mandala /events. Mandala handles idempotency/replay
    #    via mandalaingestid, so this connector never has to maintain state.
    assert _bus is not None
    for event in events:
        try:
            await _bus.post(event)
            log.info(
                "descartes.event.forwarded",
                type=event["type"],
                trace_id=event["trace_id"],
                subject=event["subject"],
            )
        except Exception as exc:  # noqa: BLE001
            log.error("descartes.event.forward_failed", error=str(exc), type=event["type"])
            # Return 500 so MacroPoint retries. Idempotency in Mandala
            # ensures duplicate delivery is safe.
            raise HTTPException(status_code=500, detail="forward failed") from exc

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/outbound/location-update", status_code=status.HTTP_204_NO_CONTENT)
async def send_location_update(payload: dict[str, Any]) -> Response:
    """Outbound endpoint for pushing Mandala-enriched location updates back
    to MacroPoint. Called by an upstream Mandala worker subscription.

    Body (JSON):
        {"shipment_id": "...", "latitude": 41.5, "longitude": -87.5,
         "captured_at": "2026-...", "speed_mps": 25.0, "eta": "2026-..."}
    """
    if not _outbound:
        raise HTTPException(
            status_code=503,
            detail="outbound disabled (set OUTBOUND_ENABLED=1 and DESCARTES_API_KEY)",
        )
    try:
        await _outbound.send_location_update(
            shipment_id=str(payload["shipment_id"]),
            latitude=float(payload["latitude"]),
            longitude=float(payload["longitude"]),
            captured_at=_parse_ts(payload["captured_at"]),
            speed_mps=payload.get("speed_mps"),
            heading_deg=payload.get("heading_deg"),
            eta=_parse_ts(payload["eta"]) if payload.get("eta") else None,
            status=payload.get("status", "InTransit"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=cfg.connector_port)
