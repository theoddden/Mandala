# Mandala

> The bridge between the wheel and the plane.

<!-- mcp-name: io.github.theoddden/mandala -->

**Mandala is an open-source logistics event bridge.** One canonical event
schema connects fleet telemetry (Samsara), trade/customs platforms
(Descartes MacroPoint, WiseTech CargoWise), rail intermodal (Vizion),
carrier safety (FMCSA SAFER), and fuel-card networks (FLEETCOR, Coast,
WEX, EFS) — and ships them as **OpenTelemetry spans**, dbt-modeled marts,
and MCP tools for LLM agents.

It does one thing very well: `POST /events` → canonical bus → projections,
materialized views, alerts, and OTel trace export. No TMS, no visibility
platform, no warehouse. Just the plumbing that makes the others talk to
each other.

```
   Samsara                                       Descartes / CargoWise / Vizion / FMCSA
  ┌────────┐    webhook       ┌──────────┐    webhook    ┌──────────┐
  │ trucks │ ───────────────▶ │  Mandala │ ◀──────────── │ shipments│
  │ sensor │                  │  bridge  │               │ customs  │
  └────────┘ ◀─────────────── │ ┌──────┐ │ ────────────▶ │ rail     │
            alerts/enrichment │ │ MCP  │ │  Samsara push └──────────┘
              ┌────────┐      │ │tools │ │
              │ Claude │ ◀────┤ └──────┘ │       Redis Streams + State (TTL)
              │  / LLM │      │ ┌──────┐ │              │
              └────────┘      │ │ OTLP │ │  ┌───────────┴────────────┐
                              │ └──────┘ │  │                        │
                              └──────────┘  ▼                        ▼
                                    │  Jaeger / Tempo /         warehouse sink
                                    │  Honeycomb / Datadog      → dbt-mandala
                                    │
                                    ▼
                          (every shipment is a trace)
```

## What's new in 0.3

- **Trace-native envelope.** Every `MandalaEvent` is also an
  OpenTelemetry span. Shipment-subject derives `trace_id`
  deterministically, so every truck/vessel/customs event for one shipment
  auto-correlates into a single distributed trace in any OTLP-compatible
  backend (Jaeger, Tempo, Honeycomb, Datadog, Grafana Cloud). See
  [Trace-native logistics](#trace-native-logistics).
- **Logistics semantic conventions.** Proposed `logistics.*` OTel
  attribute namespace (`mandala/core/events/semconv.py`) so shipments are
  filterable / groupable / aggregable using your existing observability
  stack.
- **Deterministic Event-Time Windowing.** Geometric Idempotency and the
  Stator's Latch prevent state-machine corruption from out-of-order spatial
  data. If a truck goes through a dead zone and uploads 50 pings at once
  later, the system doesn't "hallucinate" that the truck teleported. See
  [Deterministic Event-Time Windowing](#deterministic-event-time-windowing).
- **Minimum docker footprint.** 4 services, ~300MB RAM. Optional profiles
  (`otel`, `traces`, `all`) for OTel collector + Jaeger UI.
- **Focused.** Removed EPCIS adapter, IOF SCRO ontology, AIS placeholder,
  and webhook-hot-path enrichment. Enrichment now runs as **detectors in
  the worker** (FMCSA, Rail) — never blocking ingest.

## Why

Samsara has truck-level operational data. Descartes / CargoWise have
customs filings and trade intelligence. Vizion has rail intermodal status.
FMCSA SAFER has carrier CSA scores. None of them talk to each other. A
truck enters a US-Mexico POE geofence — Samsara knows; the customs broker
doesn't. A customs hold lands in Descartes — the dispatcher running
Samsara doesn't see it. **Mandala is the missing event layer.**

## What Mandala actually is

An **event-sourced integration bridge** with a short-lived Redis
projection. Not a visibility platform, not a TMS, not a data warehouse —
the plumbing that connects them.

### Architectural boundary: POST /events

Mandala's job is to be the **canonical event bus**. Your job is to get
your data into it.

**The contract:**
- `POST /events` (or use a bundled webhook connector)
- Follow the CloudEvents 1.0 schema (see [SCHEMA.md](SCHEMA.md))
- Mandala handles projection, detection, alerting, materialized views,
  and OTel span export

**What you implement yourself** (or use bundled connectors):
- ATRI bottleneck polling → POST to `/events`
- CBP ACE customs status → POST to `/events`
- AIS vessel tracking → POST to `/events`
- Postgres/MySQL CDC → POST to `/events`

Typically 20-line scripts. Mandala ships optional utilities
(`core/connector.py`, `core/scheduler.py`, `core/file_watcher.py`,
`core/cdc.py`) but the ingestion logic is yours.

### Core architecture

| Component | Purpose |
|---|---|
| `core/events/envelope.py` | CloudEvents 1.0 + OTel span envelope — the only internal shape |
| `core/events/semconv.py` | Logistics semantic conventions for OTel attributes |
| `core/bus.py` | Redis Streams pub/sub with consumer groups |
| `core/state.py` | Redis-backed projection with TTL (14-day default) |
| `core/observability/otlp_exporter.py` | Opt-in OTLP/HTTP exporter (zero overhead when disabled) |
| `core/hmac.py` | Webhook signature verification with replay protection |
| `core/dead_letter.py` | DLQ with retry policy |
| `core/alert_routing.py` | Slack / email / SMS / PagerDuty / webhook fan-out |
| `core/detector_sandbox.py` | Timeout and circuit breaker protection for detectors |
| `core/replay.py` | Event replay from Iceberg or Redis Stream for bug fixes |
| `core/adaptive_backpressure.py` | Resource-aware backpressure based on system health |
| `views/{geospatial,bitmap,timeseries,graph}.py` | Materialized views over the stream |
| `mcp/server.py` | MCP stdio server for LLM agents |

**The pattern:**
1. **Ingest** — webhook receives vendor payload → normalize to `MandalaEvent` → verify HMAC → check idempotency → publish to Redis Stream.
2. **Process** — worker reads stream → projects into `StateStore` → runs detectors → publishes alert events back to stream → emits OTel span (if enabled).
3. **Query** — MCP server reads from `StateStore` (read-only). Views runner maintains GEO / Bitmap / Timeseries / Graph indexes in its own consumer group.

### Materialized views

Mandala includes four read-optimized materialized views that subscribe to
the event stream and maintain specialized data structures in Redis:

| View | Purpose | Redis primitive |
|---|---|---|
| `GeospatialView` | "Trucks within 50km of POE" | GEO |
| `TimeseriesView` | Cold-chain readings with auto-retention | Sorted Set |
| `BitmapView` | "Trucks at POE without a customs filing" | BITMAP |
| `GraphView` | truck ↔ shipment ↔ carrier relationships | RedisGraph/FalkorDB (optional) |

Views run in a separate consumer group (`mandala:views`) so they never
back up the detector pipeline.

```bash
mandala views               # run views runner
mandala views --rebuild     # rebuild all views from scratch
```

## Trace-native logistics

A shipment's lifecycle is, by definition, a distributed trace. Factory
dispatch → freight pickup → port loading → customs clearance → vessel
transit → unloading → warehouse → last mile → delivery — each stage has
a start time, end time, attributes, and a parent context. **That is
literally an OpenTelemetry span.**

Mandala leans into this:

- Every `MandalaEvent` carries a `trace_id`, `span_id`, optional
  `parent_span_id`, optional `end_time`, and an `attributes` dict.
- The `trace_id` is `SHA256(subject)[:32]`. Every event that shares a
  shipment subject (e.g. `urn:mandala:shipment:ABC123`) **auto-correlates
  into the same trace, with no coordination.**
- Detector-emitted events inherit the source event's `span_id` as their
  `parent_span_id`, so causal chains (ingest → detector → alert) show up
  natively in any trace UI.
- The worker ships every event to OTLP when `MANDALA_OTLP_ENDPOINT` is
  set. Unset = zero overhead.

```python
from mandala.core.events.envelope import new_event
from mandala.core.events.semconv import LogisticsAttr

event = new_event(
    type="mandala.truck.poe.entered",
    source="mandala/connector/samsara",
    subject="urn:mandala:shipment:ABC123",
    attributes={
        LogisticsAttr.SHIPMENT_ID: "ABC123",
        LogisticsAttr.TRUCK_ID: "truck-42",
        LogisticsAttr.CARRIER_SCAC: "MAEU",
        LogisticsAttr.LOCATION_POE: "laredo",
    },
)
# event.trace_id == SHA256("urn:mandala:shipment:ABC123")[:32]
# event.to_otlp_span() returns an OTLP/JSON span ready for any backend
```

**What you get for free** by being trace-native:

- Jaeger / Grafana Tempo / Honeycomb / Datadog / Grafana Cloud trace
  visualization — out of the box, no Mandala dashboard required.
- Latency analysis across spans (factory→delivery P50/P95).
- Tail-based sampling, span links, error budgets — every OTel feature.
- Agent-readable causality: LLMs already reason about distributed systems
  via traces.

### Running with traces locally

```bash
# Full stack with OTel collector + Jaeger UI
docker compose --profile all up

# Just OTel collector (route to your own backend)
MANDALA_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces \
  docker compose --profile otel up

# Open Jaeger UI to browse shipment traces
open http://localhost:16686
```

Edit `deploy/otel-collector.yaml` to route to Honeycomb, Datadog, Grafana
Cloud, or any OTLP backend.

## Deterministic Event-Time Windowing

While standard SaaS uses "System Time" (when the server receives the data),
elite telemetry systems use **Event-Time Determinism**. This ensures that if
a truck goes through a dead zone and uploads 50 pings at once later, the
system doesn't "hallucinate" that the truck teleported.

### The Problem: Geometric Idempotency

If Ping A (Location: Laredo) arrives after Ping B (Location: San Antonio)
due to network lag, a naive system triggers a "Speeding Alert" or "Route
Deviation" because it thinks the truck traveled 150 miles in 1 second.

### The Solution: Stator's Latch + Geometric Hashing

Mandala implements three components to solve this:

1. **Geometric Hashing** — Derive a deterministic geometric hash (using H3 or S2)
   for each coordinate and bind it to the Event Timestamp at the source (the truck).

2. **The Stator's Latch** — A Redis-backed latch that tracks the last committed
   event time per entity. If an event arrives with an older timestamp, it's flagged
   as "time-travel" data and routed to backfill instead of triggering alerts.

3. **Re-ordering Buffer** — When events arrive out of sequence, the buffer can
   "re-wind" the state of the asset, insert the missing data point, and re-calculate
   the trajectory before the detector pipeline sees it.

### Configuration

```bash
# Enable deterministic event-time windowing
MANDALA_EVENT_TIME_DETERMINISM_ENABLED=1

# Geometric hashing (h3, s2, or none)
MANDALA_GEOMETRIC_HASH_PROVIDER=h3
MANDALA_GEOMETRIC_HASH_RESOLUTION=9

# Stator's Latch
MANDALA_STATOR_LATCH_ENABLED=1
MANDALA_STATOR_LATCH_TTL_SECONDS=1209600  # 14 days
MANDALA_STATOR_LATCH_TOLERANCE_SECONDS=1

# Re-ordering Buffer
MANDALA_REORDER_BUFFER_ENABLED=1
MANDALA_REORDER_BUFFER_MAX_EVENTS_PER_ENTITY=100
MANDALA_REORDER_BUFFER_MAX_WAIT_SECONDS=300  # 5 minutes
MANDALA_REORDER_BUFFER_EXPIRE_SECONDS=3600  # 1 hour

# Spatial coherence checks
MANDALA_SPATIAL_COHERENCE_ENABLED=1
MANDALA_MAX_VELOCITY_MPS=150.0  # ~335 mph, generous for trucks
```

### The "Military Grade" Pattern

```python
# The Stator's Latch checks event-time determinism
event_key = generate_idempotency_key(packet.source_id, packet.event_time)
last_committed_time = redis.get(f"latch:{packet.source_id}")

if packet.event_time < last_committed_time:
    # This is "Time-Travel" data. Don't trigger alerts.
    # Update the historical graph but bypass the real-time Turbine.
    return backfill_historical_graph(packet)

# Commit the new state and advance the latch
commit_state_to_paged_memory(packet)
redis.set(f"latch:{packet.source_id}", packet.event_time)
```

### Benefits

- **For Logistics:** Eliminates 99% of false "Truck Stolen" or "Geofence Breached" alerts
- **For Options:** Identical to Matching Engine Logic — if a "Cancel" order arrives
  after a "Fill" due to latency, the exchange must have a deterministic latch
- **For Audits:** Three-timestamp accounting (occurred_at, received_at, processed_at)
  proves when Mandala detected an issue relative to when the event occurred

## Hosting profiles

Mandala is intentionally tiny. The minimum stack is **4 services,
~350MB RAM** — fits on a $5/mo VPS. Everything else is opt-in via docker
compose profiles.

| Profile | Adds | RAM | Use case |
|---|---|---|---|
| (default) | redis, nginx, api, worker | ~350MB | Self-hosted, small fleet, <1k events/sec |
| `--profile ha` | redis-replica + 3x sentinel | +150MB | Self-hosted HA without AWS ElastiCache |
| `--profile otel` | otel-collector | +50MB | Route spans to your APM (Honeycomb/Datadog/Tempo) |
| `--profile traces` | otel-collector + jaeger | +250MB | Local trace browsing UI |
| `--profile all` | ha + traces | ~750MB | Full local dev with HA and trace visualization |

```bash
docker compose up                        # minimum (nginx + rate limiting)
docker compose --profile ha up           # +Redis Sentinel for HA
docker compose --profile otel up         # +OTLP export
docker compose --profile traces up       # +Jaeger UI
docker compose --profile all up          # everything
```

Trace storage (Jaeger / Tempo / Honeycomb / Datadog) is **always
external** — Mandala produces spans; it doesn't host them. This keeps the
core footprint flat regardless of trace volume.

## v0.3 scope

Fully functional out of the box with **no commercial agreements**:

- **Samsara connector** — webhook + REST client, with outbound enrichment
  push to Samsara custom fields / alerts.
- **Descartes MacroPoint connector** (public carrier docs).
- **WiseTech CargoWise connector** — eAdaptor inbound webhook (Universal
  Event XML) + outbound client to push status updates.
- **FMCSA SAFER enrichment detector** — free public API, enriches carrier
  events with live CSA scores, inspection history, OOS rate, and
  operating authority. No credentials required. Runs as a detector in
  the worker, not the webhook hot path.
- **Vizion API rail enrichment detector** — covers all 7 Class I North
  American railways (UP, BNSF, CSX, NS, CN, CPKC) with one API key.
  Container events get rail status, milestones, ETA, last free day, and
  pickup availability. Free trial available.
- **Cross-border alert engine** — fires when a truck enters a POE
  geofence with no matching customs filing.
- **Cold-chain alerts** — temperature against the declared shipment range.
- **Load-board auto-posting** (DAT, opt-in) — when a delivery lands,
  Mandala emits `mandala.truck.empty` and posts available capacity to
  every configured board.
- **Fuel-card connectors** — Coast, FLEETCOR/Comdata, WEX, EFS for
  cost-per-mile / cost-per-route analytics.
- **MCP server** — read-only tools for querying shipments, trucks,
  customs status, alerts, and materialized views.
- **dbt-mandala package** — staging + intermediate + 8 marts including
  `mandala_lane_intelligence` (proprietary lane-delay baselines from
  accumulated crossing history).
- **OTLP exporter** — opt-in trace export to any OTLP backend.
- **Single Redis dependency.** No Postgres, no Kafka.

Aurora and SAP scaffolds exist but are stubs until commercial partnerships
are in place. See `docs/integrations/aurora.md` and
`docs/integrations/sap.md`.

Mandala degrades gracefully — it must be useful with **only** Samsara
configured.

## Cross-border POE geofencing

Mandala supports configurable Port-of-Entry (POE) geofences for
cross-border operations. When a truck enters or exits a configured POE
geofence in Samsara, Mandala emits:

- `mandala.truck.poe.entered`
- `mandala.truck.poe.exited`

POE geofences are configured via `MANDALA_POE_GEOFENCES` (see
`.env.example`). Combined with Descartes MacroPoint customs events, this
provides real-time visibility into border crossings for any POE (US-MX,
US-CA, EU, anywhere).

## Customs visibility events

Granular customs status events emitted from the Descartes MacroPoint
webhook:

- `mandala.shipment.customs.hold.landed`
- `mandala.shipment.customs.hold.cleared`
- `mandala.shipment.customs.documentation.missing`
- `mandala.shipment.customs.inspection.required`

Combined with alert routing (Slack / email / SMS / PagerDuty / webhook),
these surface immediately in your tooling without manual phone calls to
customs brokers.

## Install

```bash
pip install mandala       # core
pip install 'mandala[mcp]' # +MCP server
```

## Quickstart (under an hour)

### Prerequisites

- **Docker & Docker Compose** — for running Redis, API, and worker
- **Samsara account** — fleet telemetry webhooks (free tier works)
- **Python 3.11+** — if running outside Docker (optional)

### Step 1: Clone and configure

```bash
git clone https://github.com/theoddden/Mandala
cd Mandala
cp .env.example .env
```

Minimum `.env`:

```bash
# Required: Samsara webhook
MANDALA_SAMSARA_WEBHOOK_SECRET=your-secret-here

# Recommended: push enrichment back to Samsara dashboard
MANDALA_SAMSARA_API_TOKEN=your-samsara-api-token
MANDALA_SAMSARA_OUTBOUND_ENABLED=1

# Optional: Descartes / CargoWise / Vizion / DAT
MANDALA_DESCARTES_WEBHOOK_SECRET=
MANDALA_CARGOWISE_WEBHOOK_SECRET=
MANDALA_VIZION_API_KEY=

# Optional: trace-native span export
# MANDALA_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
```

Webhook secrets default to empty strings for fail-closed security.
Mandala validates HMAC signatures and timestamps to prevent replay
attacks.

### Step 2: Start Mandala

```bash
docker compose up -d
```

Three services come up: `redis`, `api` (port 8000), `worker`. Verify:

```bash
docker compose ps
docker compose logs -f api
docker compose logs -f worker
```

### Step 3: Configure Samsara webhook

1. Samsara Admin Console → **Settings → Webhooks → Add Webhook**
2. URL: `http://YOUR_HOST:8000/webhooks/samsara`
3. Events: `Vehicle Location`, `Geofence Entry`, `Geofence Exit` (minimum)
4. Secret: same value as `MANDALA_SAMSARA_WEBHOOK_SECRET`

For local testing, use [ngrok](https://ngrok.com/): `ngrok http 8000`.

### Step 4: Verify events in the stream

```bash
docker compose exec redis redis-cli XREVRANGE mandala:events + - COUNT 10
```

You'll see CloudEvents-1.0-shaped JSON with OTel span fields:

```json
{
  "id": "uuid-v7",
  "source": "mandala/connector/samsara",
  "type": "mandala.truck.geofence.entered",
  "time": "2026-05-09T17:30:00Z",
  "subject": "urn:mandala:truck:samsara:12345",
  "trace_id": "9f3b8a...",
  "span_id": "1c4d...",
  "attributes": {
    "logistics.truck.id": "12345",
    "logistics.location.geofence": "Facility"
  },
  "data": { "...": "..." }
}
```

### Step 5: Inspect state

State store is Redis-backed with 14-day TTL:

```bash
docker compose exec redis redis-cli HGETALL "mandala:state:truck:12345"
docker compose exec redis redis-cli KEYS "mandala:state:truck:*"
```

### Step 6: (Optional) Enable trace export

```bash
# Add to .env
MANDALA_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces

# Bring up OTel collector + Jaeger UI
docker compose --profile all up -d

# Browse shipment traces
open http://localhost:16686
```

### Step 7: (Optional) MCP server for LLM agents

```bash
pip install 'mandala[mcp]'
mandala mcp
```

Claude Desktop config (`~/.claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "mandala": {
      "command": "mandala",
      "args": ["mcp"]
    }
  }
}
```

Tools: `get_shipment`, `get_truck`, `check_customs_status`,
`get_recent_alerts`, `get_fleet_near_border`, `get_trucks_at_poe_without_filing`,
`get_cold_chain_breaches`, `get_entity_neighbors`.

### Step 8: (Optional) Additional connectors

```bash
# Vizion rail (free trial)
MANDALA_VIZION_API_KEY=...

# DAT load-board auto-posting (opt-in)
MANDALA_LOADBOARD_ENABLED=1
MANDALA_DAT_CLIENT_ID=...
MANDALA_DAT_CLIENT_SECRET=...

# CargoWise eAdaptor
MANDALA_CARGOWISE_WEBHOOK_SECRET=...
MANDALA_CARGOWISE_EADAPTOR_URL=...
```

FMCSA SAFER works with no credentials — it's a public API.

### Step 9: Stop

```bash
docker compose down       # stop services
docker compose down -v    # stop and clear Redis data
```

## CLI

```bash
mandala serve     # FastAPI webhook ingest
mandala worker    # event loop: project + detect + alert + OTLP-emit
mandala views     # materialized views runner
mandala mcp       # MCP stdio server for LLMs
mandala replay    # replay historical events to fix state after bugs
```

## Production reliability features

### Detector Sandbox
All detectors run with timeout and circuit breaker protection to prevent a single buggy detector from blocking the entire worker:

- **Timeout protection**: Each detector has a configurable timeout (default 30s for standard detectors, 60s for ML/FMCSA detectors)
- **Circuit breaker**: Detectors that fail repeatedly are automatically tripped open and stop executing until they recover
- **Configuration**: `MANDALA_DETECTOR_SANDBOX_ENABLED`, `MANDALA_DETECTOR_TIMEOUT_SECONDS`, `MANDALA_DETECTOR_CIRCUIT_BREAKER_THRESHOLD`

### Event Replay
When you discover a bug in projection logic or detectors, you can replay historical events to correct state:

```bash
# Replay from Iceberg event log (requires MANDALA_EVENT_LOG_ENABLED=1)
mandala replay --from 2026-04-01T00:00:00Z --to 2026-04-15T23:59:59Z --dry-run

# Replay specific entity
mandala replay --entity "urn:mandala:truck:402" --from 2026-05-01T00:00:00Z --to 2026-05-11T23:59:59Z

# Replay recent events from Redis Stream (no Iceberg required)
mandala replay --stream --count 1000
```

Replay respects idempotency keys, so duplicate events are automatically skipped.

### Adaptive Backpressure
The worker monitors system health (Redis latency, memory usage, CPU load) and adapts processing accordingly:

- **Health checks**: Monitors Redis latency, memory percent, CPU percent
- **Adaptive batch sizing**: Reduces batch size when system is degraded
- **Ingestion rejection**: Rejects new events when system is critically degraded
- **Configuration**: `MANDALA_ADAPTIVE_BACKPRESSURE_ENABLED`, `MANDALA_REDIS_LATENCY_THRESHOLD_MS`, `MANDALA_MEMORY_THRESHOLD_PERCENT`, `MANDALA_CPU_THRESHOLD_PERCENT`

## Self-implemented data ingestion

Mandala provides optional utilities for custom data ingestion, but you
implement the logic yourself and POST to `/events`.

### Example: ATRI bottleneck polling

```python
import asyncio, httpx
from datetime import datetime, UTC

async def poll_atri():
    async with httpx.AsyncClient() as client:
        while True:
            data = (await client.get("https://atri.online.org/api/bottlenecks")).json()
            for corridor, delay in data.items():
                event = {
                    "type": "mandala.atri.bottleneck.updated",
                    "source": "custom/atri_poller",
                    "time": datetime.now(UTC).isoformat(),
                    "subject": f"urn:mandala:corridor:{corridor}",
                    "attributes": {"logistics.location.corridor": corridor},
                    "data": {"corridor": corridor, "delay_min": delay},
                }
                await client.post("http://localhost:8000/events", json=event)
            await asyncio.sleep(3600)
```

### Example: SAP file drop

```python
from mandala.core.file_watcher import FileWatcher

async def on_file(path):
    for shipment in parse_csv(path):
        await httpx.post("http://localhost:8000/events", json={
            "type": "mandala.shipment.imported",
            "source": "custom/sap_watcher",
            "subject": f"urn:mandala:shipment:{shipment['id']}",
            "data": shipment,
        })

await FileWatcher().watch_directory("sap_exports", "*.csv", on_file).start()
```

### Example: Postgres CDC

```python
from mandala.core.cdc import PostgresCDC

async def on_change(change):
    await httpx.post("http://localhost:8000/events", json={
        "type": f"mandala.{change['table']}.updated",
        "source": "custom/postgres_cdc",
        "subject": f"urn:mandala:{change['table']}:{change['data']['id']}",
        "data": change["data"],
    })

await PostgresCDC(
    connection_string="postgresql://...",
    slot_name="mandala_cdc",
    publication="mandala_pub",
    callback=on_change,
).start()
```

These are your scripts. Mandala just needs events in the right format.

## The dbt package

The Mandala worker (or your own pipeline) writes events to a warehouse
table named `raw_mandala_events`. Then in your dbt project:

```yaml
# packages.yml
packages:
  - package: theoddden/Mandala
    version: [">=0.1.0", "<0.2.0"]
```

```bash
dbt deps && dbt run --select mandala
```

Marts:

| Model | Grain | Use |
|---|---|---|
| `mandala_shipments` | shipment | single pane of glass |
| `mandala_trucks_current` | truck | latest known truck state |
| `mandala_carrier_safety_profile` | DOT number | live CSA scores, inspection history, FMCSA authority |
| `mandala_intermodal_legs` | container | rail status, ETA, last free day, milestones |
| `mandala_border_crossings` | crossing event | retroactive customs audits |
| `mandala_lane_intelligence` | lane + POE + day + hour + carrier | proprietary delay baselines from accumulated crossing history |
| `mandala_cold_chain_compliance` | breach window | regulatory liability surface |
| `mandala_carbon_per_trip` | journey | CSRD / CBAM-friendly emissions |

`mandala_lane_intelligence` is the asymmetric one: after 90 days of
operation it produces lane-delay baselines no vendor sells. This is what
incumbents charge $200K/yr to approximate from aggregated shipper data —
Mandala generates it for free from your own events.

## The schema

Every event is a [CloudEvents 1.0](https://cloudevents.io) envelope,
extended with OpenTelemetry span fields, with `type` from the `mandala.*`
registry. The full contract — versioned independently of the codebase —
is in **[SCHEMA.md](SCHEMA.md)**.

### Three-timestamp event accounting

Every `MandalaEvent` carries three timestamps for compliance, audit, and
liability tracking:

- **`time`** — when the physical event occurred (e.g. truck crossed POE)
- **`received_at`** — when Mandala's webhook received the event
- **`processed_at`** — when the worker ran detectors on the event

For insurance claims and customs disputes, the three timestamps prove
when Mandala detected an issue relative to when the event occurred:

```sql
select
    occurred_at,
    received_at,
    processed_at,
    datediff('second', occurred_at, received_at)  as detection_lag_sec,
    datediff('second', occurred_at, processed_at) as alert_lag_sec
from mandala_border_crossings
```

### OTel span fields (0.3+)

- **`trace_id`** — 16-byte hex; derived from `subject` so all events for a shipment share a trace
- **`span_id`** — 8-byte hex; derived from event `id`
- **`parent_span_id`** — causal parent (e.g. detector → emitted event)
- **`end_time`** — for spans with duration (vessel transit, customs hold)
- **`attributes`** — OTel attributes following `logistics.*` semantic conventions

Schema version: **0.3**.

## Idempotency and exactly-once delivery

The idempotency key is `SHA256(vendor + event_type + occurred_at + entity_id)`.
This handles single-vendor deduplication cleanly (e.g. Samsara retries).

Cross-vendor deduplication (Samsara and MacroPoint both emit the same
border crossing) is **not** automatic — both events are processed because
different vendors use different entity ID formats, timestamp precision,
and event semantics. If you need it, query the state store from a
detector for recent events with matching semantic criteria.

The dedup window is 14 days (matching the state store TTL).

## Terraform module

For AWS deployments:

```hcl
module "mandala" {
  source  = "theoddden/mandala/aws"
  version = "~> 0.1"

  samsara_webhook_secret = var.samsara_key
  vizion_api_key         = var.vizion_key
}
```

Provisions ElastiCache Redis (~$15/mo), two ECS Fargate tasks
(`serve` + `worker`), ALB with HTTPS, Secrets Manager, IAM least-priv,
and CloudWatch logs. **~$50-60/mo** for basic us-east-1 deployment.

See [terraform/aws/README.md](terraform/aws/README.md).

## GitHub Actions

Daily fleet intelligence reports at 6:00 AM UTC. Report types:
`cross_border_compliance`, `carrier_safety`. Output: Slack / file / stdout.
30-day artifact retention.

To enable, add `SAMSARA_API_KEY` (and optionally `SLACK_WEBHOOK_URL`) as
GitHub secrets and turn on the workflow.

## Troubleshooting

**Webhook not receiving events**
- Verify the webhook URL is reachable from the vendor's servers
- Check the `MANDALA_*_WEBHOOK_SECRET` matches the vendor config
- `docker compose logs api`
- For local testing: `ngrok http 8000`

**Worker not processing events**
- `docker compose logs worker`
- `docker compose exec redis redis-cli XLEN mandala:events`

**State store empty**
- Events must be processed by the worker before appearing in state
- Check projection errors in worker logs
- Verify the 14-day TTL hasn't expired

**Spans not appearing in Jaeger / Honeycomb**
- `MANDALA_OTLP_ENDPOINT` set? `docker compose logs worker | grep otlp`
- Collector reachable from the worker container?
- `docker compose logs otel-collector` for export errors

**Redis memory growing**
- Streams auto-trim at 100,000 messages
- State keys expire on the 14-day TTL
- `docker compose exec redis redis-cli INFO memory`

**Performance issues**
- Scale workers: `docker compose up --scale worker=3`
- Run a dedicated views runner: `docker compose up --scale views=1`
- Move to AWS via the Terraform module

## Risks & privacy

- **[RISKS.md](RISKS.md)** — Descartes API fragmentation, schema breaking
  changes, GDPR exposure, etc.
- **[DATA_PRIVACY.md](DATA_PRIVACY.md)** — Mandala is a connector library,
  not a data store. TTL'd Redis state. No phone-home.

## License

Apache 2.0 — see [LICENSE](LICENSE).

Mandala is not affiliated with Samsara Inc., The Descartes Systems Group
Inc., WiseTech Global, Vizion, FLEETCOR, Coast, WEX, or EFS. References
to those products are solely for interoperability and integration.
