"""FastAPI router for inbound Samsara webhooks.

Samsara sends a single POST with header ``X-Samsara-Signature`` containing
an HMAC-SHA256 hex digest over the raw request body, signed with the
shared secret configured in the Samsara webhook UI.
"""
from __future__ import annotations

import json

import structlog
from fastapi import APIRouter, Header, HTTPException, Request, Response, status

from mandala.connectors.samsara.normalize import normalize
from mandala.core.events.idempotency import hash_payload
from mandala.core.hmac import verify_hmac_sha256
from mandala.settings import get_settings

log = structlog.get_logger(__name__)
router = APIRouter()


@router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_samsara_webhook(
    request: Request,
    x_samsara_signature: str | None = Header(default=None, alias="X-Samsara-Signature"),
) -> Response:
    settings = get_settings()
    body = await request.body()

    if not verify_hmac_sha256(
        body=body,
        received_signature=x_samsara_signature or "",
        secret=settings.samsara_webhook_secret,
        encoding="hex",
    ):
        log.warning("samsara.webhook.invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON") from None

    events = normalize(payload)
    if not events:
        log.info("samsara.webhook.unhandled", event_type=payload.get("eventType"))
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    bus = request.app.state.bus
    idempotency = request.app.state.idempotency
    stream = settings.stream_inbound

    for event in events:
        key = event.mandalaingestid or hash_payload(event.type, event.subject or "", event.to_json())
        if not await idempotency.claim(key, ttl_seconds=86_400):
            log.info("samsara.webhook.duplicate", key=key, type=event.type)
            continue
        await bus.publish(stream, event)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
