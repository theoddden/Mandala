# Mandala Descartes MacroPoint Connector

A standalone Docker container that bridges Descartes MacroPoint to a
Mandala event bus. Lateral, modular, no source-code dependency on Mandala
itself — talks to Mandala only via the public `/events` HTTP API.

## Architectural principles

This connector follows the same architectural principles as Mandala core:

- **Modular**: standalone Docker image, drop into any Compose / K8s setup.
  Communicates with Mandala via HTTP only.
- **Trace-native**: every event carries a `trace_id` derived from its
  `subject` (URN). Auto-correlates events for a shipment across vendors
  (Samsara + MacroPoint + CargoWise) into one distributed trace, viewable
  natively in Jaeger / Tempo / Honeycomb.
- **Three-timestamp accounting**: `time` (occurred) / `received_at`
  (ingest) / `processed_at` (alert) propagated for the liability chain
  used in insurance claims and customs disputes.
- **Idempotency-friendly**: deterministic `mandalaingestid` from
  `MessageId` or body fingerprint so MacroPoint webhook retries are
  silently deduplicated by Mandala downstream — connector itself stays
  stateless.
- **HMAC verification**: webhook signatures verified with constant-time
  comparison + replay-protection timestamp tolerance window.
- **Bidirectional**: receives MacroPoint webhooks (inbound) and can push
  Mandala-enriched location updates back to MacroPoint (outbound). A
  shipment tracked by Samsara can appear in MacroPoint as a
  `LocationUpdate` automatically.
- **Stateless**: no database, no cache, no local state. Restart-safe.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `MANDALA_EVENTS_URL` | `http://mandala-api:8000/events` | Where to forward normalized events |
| `DESCARTES_WEBHOOK_SECRET` | *(required)* | Shared HMAC secret for inbound webhook verification |
| `DESCARTES_API_KEY` | *(empty)* | Bearer token for outbound LocationUpdate calls |
| `DESCARTES_MACROPOINT_BASE_URL` | `https://carrier-api.macropoint.com` | MacroPoint API base URL |
| `WEBHOOK_TIMESTAMP_TOLERANCE` | `300` | Replay-protection window (seconds) |
| `OUTBOUND_ENABLED` | `0` | Set to `1` to enable outbound bridge |
| `CONNECTOR_PORT` | `9001` | HTTP listen port |

## Endpoints

- `POST /webhooks/macropoint` — inbound MacroPoint webhook (HMAC-verified)
- `POST /outbound/location-update` — push a LocationUpdate to MacroPoint
  (only when `OUTBOUND_ENABLED=1`)
- `GET /health` — healthcheck

## Quickstart

```bash
docker build -t mandala/connector-descartes:0.3 connectors/descartes

docker run -d \
  -p 9001:9001 \
  -e MANDALA_EVENTS_URL=http://mandala-api:8000/events \
  -e DESCARTES_WEBHOOK_SECRET=$DESCARTES_WEBHOOK_SECRET \
  -e DESCARTES_API_KEY=$DESCARTES_API_KEY \
  -e OUTBOUND_ENABLED=1 \
  --name mandala-descartes-connector \
  mandala/connector-descartes:0.3
```

## Compose snippet

Add to your existing Mandala `docker-compose.yml`:

```yaml
  descartes-connector:
    build: ./connectors/descartes
    environment:
      MANDALA_EVENTS_URL: http://api:8000/events
      DESCARTES_WEBHOOK_SECRET: ${DESCARTES_WEBHOOK_SECRET}
      DESCARTES_API_KEY: ${DESCARTES_API_KEY:-}
      OUTBOUND_ENABLED: ${DESCARTES_OUTBOUND_ENABLED:-0}
    ports: ["9001:9001"]
    depends_on: [api]
    restart: unless-stopped
```

Then point your MacroPoint webhook URL at
`https://your-domain.com/webhooks/macropoint` (proxy via the Mandala
nginx) or directly at `http://<host>:9001/webhooks/macropoint`.

## Mapped event types

| MacroPoint MessageType / Status | Mandala event type |
|---|---|
| `TrackingRequest` | `mandala.shipment.booked` |
| `StatusUpdate` (Booked) | `mandala.shipment.booked` |
| `StatusUpdate` (Dispatched) | `mandala.shipment.dispatched` |
| `StatusUpdate` (PickedUp) | `mandala.shipment.picked_up` |
| `StatusUpdate` (InTransit) | `mandala.shipment.in_transit` |
| `StatusUpdate` (AtBorder) | `mandala.shipment.at_border` |
| `StatusUpdate` (Delivered) | `mandala.shipment.delivered` |
| `StatusUpdate` (Cancelled) | `mandala.shipment.cancelled` |
| Customs hold landed | `mandala.shipment.customs.hold.landed` |
| Customs hold cleared | `mandala.shipment.customs.hold.cleared` |
| Eta present in payload | + `mandala.shipment.eta.updated` (child span) |

Unknown MessageTypes are forwarded as no-ops (logged, never rejected) per
Mandala schema rule: *"unknown event types must be forwarded unchanged"*.

## License

Apache 2.0 — see Mandala root `LICENSE`.
