"""FastAPI router for inbound MacroPoint webhooks.

MacroPoint signs each webhook with HMAC-SHA256 over the raw body using
the shared secret configured in the carrier integration UI. The
signature is delivered in the ``X-MacroPoint-Signature`` header,
hex-encoded, optionally with a ``sha256=`` prefix.
"""
from __future__ import annotations

import json

import structlog
from fastapi import APIRouter, Header, HTTPException, Request, Response, status

from mandala.connectors.descartes.macropoint.normalize import normalize
from mandala.core.events.idempotency import hash_payload
from mandala.core.hmac import is_timestamp_fresh, verify_hmac_sha256
from mandala.settings import get_settings

log = structlog.get_logger(__name__)
router = APIRouter()


@router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_macropoint_webhook(
    request: Request,
    x_macropoint_signature: str | None = Header(default=None, alias="X-MacroPoint-Signature"),
    x_macropoint_timestamp: str | None = Header(default=None, alias="X-MacroPoint-Timestamp"),
    date_header: str | None = Header(default=None, alias="Date"),
) -> Response:
    settings = get_settings()
    body = await request.body()

    if not verify_hmac_sha256(
        body=body,
        received_signature=x_macropoint_signature or "",
        secret=settings.descartes_webhook_secret,
        encoding="hex",
        prefix="sha256=",
    ):
        log.warning("macropoint.webhook.invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    if settings.webhook_timestamp_tolerance_sec > 0:
        ts = x_macropoint_timestamp or date_header
        if not is_timestamp_fresh(
            ts, tolerance_sec=settings.webhook_timestamp_tolerance_sec
        ):
            log.warning("macropoint.webhook.stale_timestamp", timestamp=ts)
            raise HTTPException(status_code=401, detail="stale or missing timestamp")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON") from None

    events = normalize(payload)
    if not events:
        log.info("macropoint.webhook.unhandled", message_type=payload.get("MessageType"))
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    bus = request.app.state.bus
    idempotency = request.app.state.idempotency
    stream = settings.stream_inbound

    # Idempotency fallback key must be deterministic across webhook retries.
    # Use the raw body bytes — ``event.to_json()`` is not stable because the
    # envelope contains a freshly-generated id/received_at per parse.
    body_fingerprint = hash_payload(body)

    for event in events:
        key = event.mandalaingestid or hash_payload(
            event.type, event.subject or "", body_fingerprint
        )
        if not await idempotency.claim(key, ttl_seconds=86_400):
            log.info("macropoint.webhook.duplicate", key=key, type=event.type)
            continue
        await bus.publish(stream, event)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
