# Mandala Samsara Connector

A standalone Docker container that bridges Samsara to a Mandala event bus.
Lateral, modular, no source-code dependency on Mandala itself — talks to
Mandala only via the public `/events` HTTP API.

## Architectural principles

This connector follows the same architectural principles as Mandala core:

- **Modular**: standalone Docker image, drop into any Compose / K8s setup.
  Communicates with Mandala via HTTP only.
- **Trace-native**: every event carries a `trace_id` derived from its
  `subject` (URN). Auto-correlates events for a truck/shipment across
  vendors (Samsara + MacroPoint + CargoWise) into one distributed trace,
  viewable natively in Jaeger / Tempo / Honeycomb.
- **Three-timestamp accounting**: `time` (occurred) / `received_at`
  (ingest) / `processed_at` (alert) propagated for the liability chain
  used in insurance claims and customs disputes.
- **Idempotency-friendly**: deterministic `mandalaingestid` from
  `eventId` or body fingerprint so Samsara webhook retries are silently
  deduplicated by Mandala downstream — connector itself stays stateless.
- **HMAC verification**: webhook signatures verified with constant-time
  comparison + replay-protection timestamp tolerance window.
- **Bidirectional**: receives Samsara webhooks (inbound) and can push
  Mandala-enriched data back to Samsara via custom fields (outbound).
  MacroPoint customs status, FMCSA safety scores, etc. can appear in
  Samsara automatically.
- **POE-aware**: emits POE-specific events (`TRUCK_POE_ENTERED` /
  `TRUCK_POE_EXITED`) when geofence names match configured Ports-of-Entry.
- **Stateless**: no database, no cache, no local state. Restart-safe.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `MANDALA_EVENTS_URL` | `http://mandala-api:8000/events` | Where to forward normalized events |
| `SAMSARA_WEBHOOK_SECRET` | *(required)* | Shared HMAC secret for inbound webhook verification |
| `SAMSARA_API_TOKEN` | *(empty)* | Bearer token for outbound custom field updates |
| `SAMSARA_OUTBOUND_ENABLED` | `0` | Set to `1` to enable outbound bridge |
| `POE_GEOFENCES` | `{}` | JSON dict of POE geofence names (case-insensitive) |
| `WEBHOOK_TIMESTAMP_TOLERANCE` | `300` | Replay-protection window (seconds) |
| `CONNECTOR_PORT` | `9000` | HTTP listen port |

## Endpoints

- `POST /webhooks/samsara` — inbound Samsara webhook (HMAC-verified)
- `POST /outbound/custom-field` — update a Samsara custom field
  (only when `SAMSARA_OUTBOUND_ENABLED=1`)
- `GET /health` — healthcheck

## Quickstart

```bash
docker build -t mandala/connector-samsara:0.3 connectors/samsara

docker run -d \
  -p 9000:9000 \
  -e MANDALA_EVENTS_URL=http://mandala-api:8000/events \
  -e SAMSARA_WEBHOOK_SECRET=$SAMSARA_WEBHOOK_SECRET \
  -e SAMSARA_API_TOKEN=$SAMSARA_API_TOKEN \
  -e SAMSARA_OUTBOUND_ENABLED=1 \
  -e POE_GEOFENCES='{"Laredo Crossing": {}, "Otay Mesa": {}}' \
  --name mandala-samsara-connector \
  mandala/connector-samsara:0.3
```

## Compose snippet

Add to your existing Mandala `docker-compose.yml`:

```yaml
  samsara-connector:
    build: ./connectors/samsara
    environment:
      MANDALA_EVENTS_URL: http://api:8000/events
      SAMSARA_WEBHOOK_SECRET: ${SAMSARA_WEBHOOK_SECRET}
      SAMSARA_API_TOKEN: ${SAMSARA_API_TOKEN:-}
      SAMSARA_OUTBOUND_ENABLED: ${SAMSARA_OUTBOUND_ENABLED:-0}
      POE_GEOFENCES: ${POE_GEOFENCES:-{}}
    ports: ["9000:9000"]
    depends_on: [api]
    restart: unless-stopped
```

Then point your Samsara webhook URL at
`https://your-domain.com/webhooks/samsara` (proxy via the Mandala
nginx) or directly at `http://<host>:9000/webhooks/samsara`.

## Mapped event types

| Samsara eventType | Mandala event type |
|---|---|
| `VehicleLocation`, `VehicleGpsUpdated` | `mandala.truck.position` |
| `VehicleEnterGeofence`, `GeofenceEntry` | `mandala.truck.geofence.entered` |
| `VehicleExitGeofence`, `GeofenceExit` | `mandala.truck.geofence.exited` |
| `VehicleTemperature`, `TemperatureSensorReading` | `mandala.cold_chain.reading` |
| `TemperatureExceeded` | `mandala.cold_chain.breach` |
| `EldHosViolation`, `HosViolationDetected` | `mandala.driver.log.violation` |
| `HarshEvent`, `VehicleHarshEvent` | `mandala.truck.harsh_event` |

### POE-specific events

When a geofence name matches an entry in `POE_GEOFENCES`, the connector
emits an additional event:

| Condition | Mandala event type |
|---|---|
| Truck enters POE geofence | `mandala.truck.poe.entered` |
| Truck exits POE geofence | `mandala.truck.poe.exited` |

These events have a `parent_span_id` linking them to the base geofence
event, so the causation chain shows up natively in trace UIs.

Unknown EventTypes are forwarded as no-ops (logged, never rejected) per
Mandala schema rule: *"unknown event types must be forwarded unchanged"*.

## Outbound enrichment

When `SAMSARA_OUTBOUND_ENABLED=1`, the connector exposes
`POST /outbound/custom-field` for updating Samsara custom fields with
Mandala-enriched data:

```json
{
  "vehicle_id": "281474976710656",
  "field_id": "custom_field_id",
  "value": "custom_status_value"
}
```

This enables cross-vendor enrichment: MacroPoint customs status can be
pushed to Samsara as a custom field, FMCSA safety scores can appear in
Samsara, etc. A Mandala worker subscription would call this endpoint
when relevant events are processed.

## License

Apache 2.0 — see Mandala root `LICENSE`.
