"""FastAPI router for inbound CargoWise eAdaptor webhooks.

CargoWise Outbound Subscriptions can be configured to POST Universal*
XML documents to a partner endpoint. The default authentication is HTTP
Basic, but Mandala accepts an HMAC-SHA256 hex digest in the
``X-CargoWise-Signature`` header as a defence-in-depth check (configure
the same shared secret in your CargoWise outbound subscription).

The endpoint accepts ``Content-Type: application/xml`` or ``text/xml``.
"""
from __future__ import annotations

import structlog
from fastapi import APIRouter, Header, HTTPException, Request, Response, status

from mandala.connectors.cargowise.normalize import normalize
from mandala.core.events.idempotency import hash_payload
from mandala.core.hmac import verify_hmac_sha256
from mandala.settings import get_settings

log = structlog.get_logger(__name__)
router = APIRouter()


@router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_cargowise_webhook(
    request: Request,
    x_cargowise_signature: str | None = Header(default=None, alias="X-CargoWise-Signature"),
) -> Response:
    settings = get_settings()
    body = await request.body()

    if not verify_hmac_sha256(
        body=body,
        received_signature=x_cargowise_signature or "",
        secret=settings.cargowise_webhook_secret,
        encoding="hex",
        prefix="sha256=",
    ):
        log.warning("cargowise.webhook.invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    events = normalize(body)
    if not events:
        log.info("cargowise.webhook.unhandled")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    bus = request.app.state.bus
    idempotency = request.app.state.idempotency
    stream = settings.stream_inbound

    for event in events:
        key = event.mandalaingestid or hash_payload(event.type, event.subject or "", event.to_json())
        if not await idempotency.claim(key, ttl_seconds=86_400):
            log.info("cargowise.webhook.duplicate", key=key, type=event.type)
            continue
        await bus.publish(stream, event)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
