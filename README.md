# Mandala

<!-- mcp-name: io.github.theoddden/mandala -->

Mandala is an event-sourced logistics integration bridge. It normalizes
data from fleet telemetry (Samsara), trade/customs platforms (Descartes
MacroPoint, WiseTech CargoWise), rail intermodal (Vizion), carrier safety
(FMCSA SAFER), and fuel-card networks (FLEETCOR, Coast, WEX, EFS) into a
single canonical event schema.

Events are shipped as OpenTelemetry spans to observability backends,
materialized into dbt models for analytics, and exposed via MCP tools for
LLM agents.

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

Logistics data is fragmented across vendors. Samsara has truck telemetry,
Descartes/CargoWise have customs filings, Vizion has rail status, FMCSA
has carrier safety data. These systems don't integrate. A truck enters a
border POE geofence — Samsara records it, but the customs broker doesn't
see it. A customs hold lands in Descartes — the dispatcher using Samsara
doesn't know.

Mandala provides a canonical event layer that connects these systems.

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
| `core/bus.py` | Redis Streams pub/sub with consumer groups + dual-write to Iceberg |
| `core/state.py` | Redis-backed projection with TTL (14-day default) |
| `core/observability/otlp_exporter.py` | Opt-in OTLP/HTTP exporter (zero overhead when disabled) |
| `core/hmac.py` | Webhook signature verification with replay protection |
| `core/dead_letter.py` | DLQ with retry policy and exponential backoff |
| `core/alert_routing.py` | Slack / email / SMS / PagerDuty / webhook fan-out |
| `core/detector_sandbox.py` | Timeout and circuit breaker protection for detectors |
| `core/replay.py` | Event replay from Iceberg or Redis Stream for bug fixes |
| `core/adaptive_backpressure.py` | Resource-aware backpressure based on system health |
| `core/geometric_hash.py` | H3/S2 geometric hashing for spatial idempotency |
| `core/stator_latch.py` | Event-time determinism latch for out-of-order data |
| `core/reorder_buffer.py` | Re-ordering buffer for out-of-order event handling |
| `core/circuit_breaker.py` | Circuit breaker for external API calls |
| `core/rate_limiter.py` | Token bucket rate limiting for API endpoints |
| `views/{geospatial,bitmap,timeseries,graph}.py` | Materialized views over the stream |
| `mcp/server.py` | MCP stdio server for LLM agents (8 tools) |

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

## Rust Acceleration Layer

Mandala includes an optional Rust extension (`mandala-rust-ext`) that accelerates critical cryptographic and data processing operations for high-throughput scenarios (10k+ events/sec). The extension is optional - Mandala falls back to pure Python if not installed.

**Accelerated operations:**

- **SHA256 hashing** - trace_id, span_id, and idempotency key derivation
- **HMAC-SHA256 webhook signature verification** - constant-time comparison for replay protection
- **H3 geometric hashing** - spatial idempotency for event-time determinism (optional feature)
- **Bitmap URNs conversion** - zero-copy bit manipulation for bitmap views (2-5x faster)
- **Graph result decoding** - RedisGraph/FalkorDB response parsing with automatic byte decoding
- **Geometric hash fallbacks** - float-to-bits conversion, geohash and S2 hash implementations when geometry libraries are unavailable

**Installation:**

```bash
# Install with Rust acceleration
pip install 'mandala[rust]'

# Or build from source
cd mandala-rust-ext
maturin build --release
pip install target/wheels/mandala_rust_ext-*.whl
```

**Performance impact:**

- 2-5x faster cryptographic operations vs pure Python
- 2-5x faster bitmap bit manipulation
- Zero-copy memory handling for large event batches
- Memory-safe Rust implementation with no FFI overhead

## Compliance

Mandala includes optional compliance features for GDPR/CCPA/SOC2 requirements. All features are lightweight, opt-in, and designed to minimize operational impact.

### Immutable Audit Logging

Leverages the existing Apache Iceberg event log for permanent, immutable event storage. When enabled for compliance, Mandala forces dual-write to Iceberg for all events.

**Configuration:**

```bash
# Enable immutable audit logging (forces Iceberg write)
MANDALA_AUDIT_LOG_ENABLED=1

# Iceberg configuration (required)
MANDALA_EVENT_LOG_ENABLED=1
MANDALA_ICEBERG_CATALOG=rest
MANDALA_ICEBERG_CATALOG_URI=http://localhost:8181
MANDALA_ICEBERG_WAREHOUSE=s3://mandala-events/
MANDALA_ICEBERG_TABLE=mandala.events
MANDALA_ICEBERG_NAMESPACE=mandala
```

**Benefits:**
- Append-only storage with snapshot isolation
- Time travel queries for audit/compliance
- Direct query from Snowflake, DuckDB, Spark, Trino, ClickHouse
- Schema evolution without breaking queries

### Access Logging

Logs all `/events` POST requests to a dedicated Redis stream (`mandala:audit:access`) for audit trail purposes. Separate from the main event stream to avoid pollution.

**Configuration:**

```bash
MANDALA_AUDIT_ACCESS_LOG_ENABLED=1
```

**Logged fields:**
- Timestamp
- Client IP address
- Request path
- Event type (if available)
- Subject (if available)
- User agent

**Query access logs:**

```bash
docker compose exec redis redis-cli XREVRANGE mandala:audit:access + - COUNT 10
```

### PII Detection

Scans event data for common PII patterns (emails, SSNs, phone numbers, credit cards) and emits alert events when PII is detected. Runs in the detector sandbox with timeout and circuit breaker protection.

**Configuration:**

```bash
MANDALA_PII_DETECTION_ENABLED=1
```

**Detected patterns:**
- Email addresses
- US SSN format (XXX-XX-XXXX)
- US phone numbers (XXX-XXX-XXXX)
- International phone numbers
- Credit card numbers
- IP addresses

**Alert event emitted:** `mandala.privacy.pii.detected`

### Data Residency Checks

Rejects events from disallowed geographic regions based on location attributes in the event. Configured via ISO 3166-1 alpha-2 country codes.

**Configuration:**

```bash
MANDALA_DATA_RESIDENCY_ENABLED=1
MANDALA_DATA_RESIDENCY_ALLOWED_REGIONS=US,CA,MX  # North America
```

**Checked locations:**
- `attributes.logistics.location.country`
- `data.country`
- `data.location.country`
- `data.address.country`

**Response:** Events from disallowed regions receive HTTP 403 Forbidden.


### Change Tracking

Tracks state changes by comparing current event data with prior state from Redis. Emits audit events when significant field changes are detected.

**Configuration:**

```bash
MANDALA_CHANGE_TRACKING_ENABLED=1
```

**Audit event emitted:** `mandala.audit.state.changed`

**Tracked changes:**
- All fields by default
- Optional field whitelist for targeted tracking

### Compliance Feature Summary

| Feature | Purpose | Impact | Env Var |
|---|---|---|---|
| Immutable Audit Logging | Permanent event storage for compliance | Iceberg dual-write | `MANDALA_AUDIT_LOG_ENABLED` |
| Access Logging | Audit trail of all event ingest | Redis stream write | `MANDALA_AUDIT_ACCESS_LOG_ENABLED` |
| PII Detection | Scan events for PII patterns | Detector sandbox | `MANDALA_PII_DETECTION_ENABLED` |
| Data Residency | Reject events from disallowed regions | Middleware check | `MANDALA_DATA_RESIDENCY_ENABLED` |
| Change Tracking | Track state changes for audit | Detector sandbox | `MANDALA_CHANGE_TRACKING_ENABLED` |

**All compliance features are disabled by default. Enable only what you need.**

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
`get_cold_chain_breaches`, `get_entity_neighbors`, `get_trailer_handoff_chain`,
`get_shipment_via_trailer`.

**Example: Fleet optimization in 30 seconds**

```
Export dbt-mandala/mart_fleet_performance.csv, then ask Claude:
"Analyze this fleet data and identify the 3 highest-impact opportunities to reduce dwell time at Laredo POE."
```

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

## Production Readiness Status

**Code Quality:** Production-ready. The codebase includes comprehensive reliability features:
- Circuit breakers for external API calls
- Adaptive backpressure based on system health
- Rate limiting via token bucket
- Dead Letter Queue with exponential backoff retry
- Detector sandbox with timeout and circuit breaker protection
- Event replay from Iceberg or Redis Stream
- Three-timestamp accounting for compliance

**CI/CD Pipeline:** Not production-grade. The current pipeline has basic testing but lacks critical enterprise features:
- Security failures are ignored (bandit, safety, mypy)
- No deployment automation (Docker build, Kubernetes deployment)
- No integration/E2E tests (only unit tests with mocks)
- Coverage threshold too low (40% vs. 60-80% for production)
- No secret scanning, container scanning, or SBOM generation

**Estimated time to production-grade CI/CD:** 6-8 weeks with dedicated DevOps engineer.

**Self-hosted deployment:** Ready for small fleets (<1k events/sec). The default stack (4 services, ~350MB RAM) works on a $5/mo VPS. HA profile adds Redis Sentinel support.

**Enterprise deployment:** Requires Terraform or manual Kubernetes deployment. The Terraform module exists but is not validated in CI.

**Recommendation:** For production use, implement the CI/CD improvements documented in `CI_CD_AUDIT.md` before enterprise deployment.

## Production reliability features

### Circuit Breakers
External API calls (FMCSA, Vizion, Samsara outbound) are protected by circuit
breakers that prevent cascading failures when downstream services are degraded.

### Rate Limiting
API endpoints are protected by token bucket rate limiting to prevent abuse and
protect against webhook floods. Configurable via `MANDALA_RATE_LIMIT_ENABLED`,
`MANDALA_RATE_LIMIT_REQUESTS_PER_MINUTE`, and `MANDALA_RATE_LIMIT_BURST_SIZE`.

### Dead Letter Queue
Failed events are sent to a DLQ with exponential backoff retry policy.
Failed events can be inspected and re-processed after fixing the root cause.

### Detector Sandbox
All detectors run with timeout and circuit breaker protection to prevent a
single buggy detector from blocking the entire worker:

- **Timeout protection**: Each detector has a configurable timeout (default 30s
  for standard detectors, 60s for ML/FMCSA detectors)
- **Circuit breaker**: Detectors that fail repeatedly are automatically tripped
  open and stop executing until they recover
- **Configuration**: `MANDALA_DETECTOR_SANDBOX_ENABLED`,
  `MANDALA_DETECTOR_TIMEOUT_SECONDS`,
  `MANDALA_DETECTOR_CIRCUIT_BREAKER_THRESHOLD`

### Event Replay
When bugs are discovered in projection logic or detectors, historical events can
be replayed to correct state:

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
The worker monitors system health (Redis latency, memory usage, CPU load) and
adapts processing accordingly:

- **Health checks**: Monitors Redis latency, memory percent, CPU percent
- **Adaptive batch sizing**: Reduces batch size when system is degraded
- **Ingestion rejection**: Rejects new events when system is critically degraded
- **Configuration**: `MANDALA_ADAPTIVE_BACKPRESSURE_ENABLED`,
  `MANDALA_REDIS_LATENCY_THRESHOLD_MS`, `MANDALA_MEMORY_THRESHOLD_PERCENT`,
  `MANDALA_CPU_THRESHOLD_PERCENT`

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


##The Mandala worker (or your own pipeline) writes events to a warehouse
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

`mandala_lane_intelligence` produces lane-delay baselines from accumulated
crossing history. After 90 days of operation, it generates baselines that
are typically sold as proprietary data by incumbents.

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
## Standalone Docker Connectors

Samsara and Descartes connectors can run as independent Docker containers
without the full Mandala stack. This is useful for:

- Running connectors in separate infrastructure
- Testing connectors in isolation
- Integrating with existing event pipelines

```bash
# Samsara connector standalone
docker compose -f docker-compose.samsara-connector.yml up

# Descartes connector standalone
docker compose -f docker-compose.descartes-connector.yml up
```

## Hosting profiles

Mandala is designed for minimal resource usage. The default stack is 4 services
with ~350MB RAM, suitable for a $5/mo VPS. Additional features are opt-in via
docker compose profiles.

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

Provisions ElastiCache Redis (~$15/mo), two ECS Fargate tasks
(`serve` + `worker`), ALB with HTTPS, Secrets Manager, IAM least-priv,
and CloudWatch logs. **~$50-60/mo** for basic us-east-1 deployment.

See [terraform/aws/README.md](terraform/aws/README.md).

## Troubleshooting

**Webhook not receiving events**
- Verify the webhook URL is reachable from the vendor's servers
- Check that `MANDALA_*_WEBHOOK_SECRET` matches the vendor config
- Check API logs: `docker compose logs api`
- For local testing: use `ngrok http 8000`

**Worker not processing events**
- Check worker logs: `docker compose logs worker`
- Check stream length: `docker compose exec redis redis-cli XLEN mandala:events`
- Verify consumer group exists: `docker compose exec redis redis-cli XINFO GROUPS mandala:events`

**State store empty**
- Events must be processed by the worker before appearing in state
- Check for projection errors in worker logs
- Verify the 14-day TTL hasn't expired

**Spans not appearing in Jaeger / Honeycomb**
- Verify `MANDALA_OTLP_ENDPOINT` is set
- Check worker logs for OTLP errors: `docker compose logs worker | grep otlp`
- Verify collector is reachable from the worker container
- Check collector logs: `docker compose logs otel-collector`

**Redis memory growing**
- Streams auto-trim at 100,000 messages (configurable via `MANDALA_STREAM_MAXLEN`)
- State keys expire on the 14-day TTL (configurable via `MANDALA_STATE_TTL_SECONDS`)
- Check memory usage: `docker compose exec redis redis-cli INFO memory`

**Performance issues**
- Scale workers: `docker compose up --scale worker=3`
- Run a dedicated views runner: `docker compose up --scale views=1`
- Adjust batch size: `MANDALA_STREAM_BATCH_SIZE`
- Move to AWS via the Terraform module for higher throughput

**Circuit breaker tripping**
- Check which external APIs are failing
- Verify API credentials are valid
- Check rate limits on downstream services
- Monitor circuit breaker state in logs

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
