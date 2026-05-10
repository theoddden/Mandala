# Mandala

> The bridge between the wheel and the plane.

**Mandala** is an open-source event bridge that connects fleet telemetry
(Samsara) and trade/customs platforms (Descartes — starting with
MacroPoint) through a single canonical event schema. It ships in two
forms simultaneously:

- a **Python service** (`mandala`) — webhook ingest + Redis-Streams
  worker + MCP server for LLM agents.
- a **dbt package** (`dbt-mandala`) — `dbt deps` and you have warehouse-
  native `mandala_shipments`, `mandala_trucks_current`,
  `mandala_border_crossings`, `mandala_cold_chain_compliance`,
  `mandala_carbon_per_trip` materializing in your warehouse.

```
   Samsara                                  Descartes / MacroPoint
  ┌────────┐    webhook     ┌──────────┐    webhook     ┌──────────┐
  │ trucks │ ─────────────▶ │  Mandala │ ─────────────▶ │ shipments│
  │ sensor │                │  bridge  │                │ customs  │
  └────────┘ ◀───────────── │   ┌────┐ │ ◀───────────── └──────────┘
              alerts/route  │   │MCP │ │   holds/BOL
              ┌────────┐    │   │tool│ │
              │ Claude │ ◀──┤   │  s │ │       Redis Streams
              │  / LLM │    │   └────┘ │            │
              └────────┘    └──────────┘            ▼
                                            warehouse sink ──▶ dbt-mandala
```

## Why

Samsara has truck-level operational data. Descartes has customs filings,
carrier networks, and trade intelligence. They don't talk to each other.
A truck enters a US-Mexico Port-of-Entry geofence — Samsara knows; the
customs broker doesn't necessarily. A customs hold lands in Descartes —
the dispatcher running Samsara doesn't see it. Mandala is the missing
event layer.

## What Mandala Actually Is

Mandala is an **event-sourced integration bridge** with a short-lived Redis projection. It's not a visibility platform, not a TMS, and not a data warehouse — it's the plumbing that connects them.

### Core Architecture (~240 lines)

| Component | Purpose | Lines |
|---|---|---|
| `core/events/envelope.py` | CloudEvents 1.0 wrapper — the only internal data shape | 114 |
| `core/bus.py` | Redis Streams pub/sub with consumer groups | 110 |
| `core/state.py` | Redis-backed projection with TTL (14-day default) + field clearing via `STATE_DELETE` | 65 |

**The pattern:**
1. **Ingest** — webhook receives vendor payload → normalize to `MandalaEvent` → publish to Redis Stream
2. **Process** — single worker reads stream → projects into `StateStore` → runs detectors → publishes alerts back to stream
3. **Query** — MCP server reads from `StateStore` (read-only, no writes)

### Materialized Views

Mandala includes four read-optimized materialized views that subscribe to the event stream and maintain specialized data structures in Redis:

| View | Purpose | Redis Data Structure |
|---|---|---|
| `GeospatialView` | Index truck positions for spatial queries (e.g., "trucks within 50km of POE") | GEO |
| `TimeseriesView` | Index cold-chain readings with retention trimming | Sorted Set |
| `BitmapView` | Track truck presence at POEs and customs filing status via bit operations | BITMAP |
| `GraphView` | Project entity relationships (truck ↔ shipment ↔ carrier) into a graph | RedisGraph/FalkorDB (optional) |

**Benefits:**
- O(1) spatial queries instead of O(N) scans
- Boolean set algebra for complex conditions (e.g., "trucks at POE without filing")
- Time-series queries with automatic retention
- Eventually consistent with the event stream

**Usage:**
```bash
mandala views                    # Run views runner
mandala views --rebuild          # Rebuild all views from scratch
```

Views run in a separate consumer group (`mandala:views`) so they never back up the detector pipeline.

## v0.1 scope

Fully functional out of the box with **no commercial agreements**:

- **Samsara connector** — webhook + REST client.
- **Descartes MacroPoint connector** (public carrier docs).
- **WiseTech CargoWise connector** — eAdaptor inbound webhook (Universal
  Event XML) + outbound client to push status updates back into
  CargoWise. Sits alongside Descartes; either or both can be enabled.
- **FMCSA SAFER enrichment** — free, public API that enriches carrier
  events with live CSA scores, inspection history, violation records,
  out-of-service rate, and operating authority status. No credentials
  required. Decorates carrier events with FMCSA data when DOT number is
  present.
- **Rail intermodal enrichment (Vizion API)** — covers all 7 Class I
  North American railways (UP, BNSF, CSX, NS, CN, CPKC) with a single
  API key. No LOA required. Enriches container events with rail status,
  milestones, ETA, last free day, and availability for pickup. Free trial
  available.
- **Cross-border alert engine** — fires when a truck enters a POE
  geofence with no matching customs filing.
- **Cold-chain alerts** — temperature against the declared shipment
  range.
- **Load-board auto-posting** (DAT + Truckstop, **opt-in**) — when a
  delivery lands, Mandala emits `mandala.truck.empty` and
  posts available capacity to every configured board with the truck's
  current GPS position and equipment type. Disabled by default
  (`MANDALA_LOADBOARD_ENABLED=0`); requires partner credentials per board.
- **MCP server** — read-only tools for querying shipments, trucks, customs status, alerts, and materialized views (geospatial, timeseries, bitmap, graph).
- **dbt-mandala package** — staging + intermediate + 7 marts.
- **Single Redis dependency.** No Postgres, no Kafka, no K8s.

Datamyne and Visual Compliance scaffolds exist but are stubs until
commercial partnerships are in place. Mandala degrades gracefully — it
must be useful with **only** Samsara configured.

## Install

```bash
pip install mandala       # core
pip install 'mandala[mcp]' # +MCP server
```

## Quickstart (under an hour)

### What Mandala Does (Simple Version)

Mandala connects your Samsara fleet data to trade/customs systems (Descartes) and pushes enrichment back to your Samsara dashboard. No separate Mandala dashboard needed.

**Samsara Dashboard Integration:**
- Custom field "Customs Status" shows "FILED" or "NO_FILING"
- Custom field "Last Border Crossing" shows POE and timestamp
- Custom field "Carrier CSA Score" shows safety rating
- Alerts appear in Samsara for missing customs filings, cold chain breaches, carrier safety issues

**How It Works:**
```
Samsara Webhook → Mandala → Descartes/FMCSA → Samsara Dashboard
                          (enrichment)    (push back)
```

### Prerequisites

- **Docker & Docker Compose** — for running Redis, API, and worker
- **Samsara account** — for fleet telemetry webhooks (free tier works)
- **Python 3.11+** — if running outside Docker (optional)
- **Redis CLI** — for verifying events (optional, `brew install redis` on macOS)

### Step 1: Clone and Configure

```bash
git clone https://github.com/theoddden/Mandala
cd Mandala
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# Required for Samsara webhook
MANDALA_SAMSARA_WEBHOOK_SECRET=your-secret-here

# Required for Samsara outbound integration (push enrichment back to Samsara dashboard)
MANDALA_SAMSARA_API_TOKEN=your-samsara-api-token
MANDALA_SAMSARA_OUTBOUND_ENABLED=1  # Set to 1 to enable Samsara dashboard integration
MANDALA_SAMSARA_BASE_URL=https://api.samsara.com

# Optional: Descartes MacroPoint (trade/customs)
MANDALA_DESCARTES_WEBHOOK_SECRET=
MANDALA_DESCARTES_API_KEY=
MANDALA_DESCARTES_BASE_URL=https://gln.descartes.com

# Optional: FMCSA SAFER (carrier enrichment — no credentials required)
# Just enable the connector in your workflow

# Optional: Vizion API (rail intermodal — single API key, free trial)
MANDALA_VIZION_API_KEY=
```

**Important**: 
- Set `MANDALA_SAMSARA_WEBHOOK_SECRET` to a random string. This is the HMAC secret Samsara uses to sign webhooks. You'll configure the same value in Samsara's webhook UI.
- Set `MANDALA_SAMSARA_API_TOKEN` to your Samsara API token (from Samsara Admin Console → API Tokens).
- Set `MANDALA_SAMSARA_OUTBOUND_ENABLED=1` to push enrichment back to your Samsara dashboard (custom fields + alerts).
- Webhook secrets default to empty strings for fail-closed security. Mandala validates HMAC signatures and timestamps to prevent replay attacks.

### Step 2: Start Mandala

```bash
docker compose up -d
```

This starts three services:
- **redis** — Redis 7-alpine (event stream + state store)
- **api** — FastAPI webhook ingest (port 8000)
- **worker** — Event processor (projection + alerts)

Verify services are running:

```bash
docker compose ps
# Should show all three services as "healthy"
```

Check logs:

```bash
docker compose logs -f api
docker compose logs -f worker
```

### Step 3: Configure Samsara Webhook

1. Log into Samsara Admin Console
2. Navigate to **Settings → Webhooks**
3. Click **Add Webhook**
4. Configure:
   - **URL**: `http://YOUR_HOST:8000/webhooks/samsara`
   - **Events**: Select at least `Vehicle Location`, `Geofence Entry`, `Geofence Exit`
   - **Secret**: Use the same value as `MANDALA_SAMSARA_WEBHOOK_SECRET` in your `.env`
5. Click **Save**

**Note**: Replace `YOUR_HOST` with your actual hostname or IP. For local testing, use `http://localhost:8000/webhooks/samsara` if Samsara can reach your machine (requires ngrok or similar for external access).

### Step 4: Trigger a Test Event

The easiest way to test is to trigger a geofence event in Samsara:

1. Create a simple geofence around your facility in Samsara
2. Drive a truck through the geofence (or simulate via Samsara's test webhook feature)
3. Watch Mandala logs:

```bash
docker compose logs -f worker
```

You should see log messages like:
```
INFO mandala.worker - received event, type=mandala.truck.geofence.entered, truck_id=12345
INFO mandala.worker - projected into state store
```

### Step 5: Verify Events in Redis Stream

Use Redis CLI to inspect the stream:

```bash
# Connect to Redis container
docker compose exec redis redis-cli

# Read from the event stream
XREAD STREAMS mandala:events 0

# Or read the last 10 events
XREVRANGE mandala:events + - COUNT 10
```

You'll see JSON events in CloudEvents 1.0 format:

```json
{
  "id": "uuid-v7",
  "source": "mandala/connector/samsara",
  "type": "mandala.truck.geofence.entered",
  "time": "2026-05-09T17:30:00Z",
  "subject": "urn:mandala:truck:samsara:12345",
  "data": {
    "truck_id": "12345",
    "geofence_id": "geo-1",
    "geofence_name": "Facility",
    "occurred_at": "2026-05-09T17:29:45Z"
  }
}
```

### Step 6: Check State Store

Mandala projects events into a Redis-backed state store with 14-day TTL:

```bash
# Get current state for a truck
docker compose exec redis redis-cli HGETALL "mandala:state:truck:12345"

# List all trucks in state
docker compose exec redis redis-cli KEYS "mandala:state:truck:*"
```

### Step 7: Test MCP Server (Optional)

If you want to query Mandala from an LLM:

```bash
# Install Mandala with MCP support
pip install 'mandala[mcp]'

# Start MCP server
mandala mcp
```

Add to your Claude Desktop config (`~/.claude/claude_desktop_config.json`):

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

Available tools:
- `get_shipment` — Query shipment by ID
- `get_truck` — Query truck by ID
- `check_customs_status` — Check customs filing status
- `get_recent_alerts` — Get recent alerts
- `get_fleet_near_border` — Get trucks near POE geofences

### Step 8: Enable Samsara Dashboard Integration (Recommended)

This pushes Mandala enrichment back to your Samsara dashboard. No separate Mandala dashboard needed.

**What appears in Samsara:**
- Custom field "mandala_alert_status" shows alert type and severity
- Alerts appear in Samsara for customs compliance, cold chain, carrier safety
- All enrichment happens automatically when Mandala detects issues

**Configuration:**
```bash
# Already set in Step 1, but verify:
MANDALA_SAMSARA_OUTBOUND_ENABLED=1
MANDALA_SAMSARA_API_TOKEN=your-samsara-api-token
```

**Restart Mandala:**
```bash
docker compose restart worker
```

**Verify in Samsara:**
1. Log into Samsara Admin Console
2. Navigate to **Fleet → Vehicles**
3. Click on a truck that crossed a border
4. Check custom field "mandala_alert_status" — should show alert info
5. Check **Alerts** tab — should show Mandala alerts

### Step 9: Enable Additional Connectors (Optional)

**FMCSA SAFER** (carrier enrichment, no credentials):

```bash
# Add to your workflow to enrich carrier events with CSA scores
# No configuration needed — uses public FMCSA API
```

**Vizion API** (rail intermodal):

```bash
# Add to .env
MANDALA_VIZION_API_KEY=your-vizion-key

# Free trial: https://www.vizionapi.com
```

**Load-board auto-posting** (DAT + Truckstop):

```bash
# Add to .env
MANDALA_LOADBOARD_ENABLED=1
MANDALA_DAT_CLIENT_ID=
MANDALA_DAT_CLIENT_SECRET=
MANDALA_TRUCKSTOP_INTEGRATION_ID=
MANDALA_TRUCKSTOP_USERNAME=
MANDALA_TRUCKSTOP_PASSWORD=
```

**Palantir Foundry** (ontology integration):

```bash
# Add to .env
MANDALA_PALANTIR_ENABLED=1
MANDALA_PALANTIR_API_URL=https://your-foundry.palantir.com
MANDALA_PALANTIR_TOKEN=your-foundry-token

# Start connector
docker compose --profile palantir up -d
```

**Kinaxis Maestro** (disruption integration):

```bash
# Add to .env
MANDALA_KINAXIS_ENABLED=1
MANDALA_KINAXIS_API_URL=https://your-kinaxismaestro.kinaxis.com
MANDALA_KINAXIS_API_KEY=your-kinaxis-api-key

# Start connector
docker compose --profile kinaxis up -d
```

### Step 10: Stop and Cleanup

```bash
# Stop all services
docker compose down

# Stop and remove volumes (clears Redis data)
docker compose down -v

# View logs after stopping
docker compose logs
```

## FAQ

**How does the idempotency key actually work when the same physical event comes from two different connectors?**

The idempotency key is derived from `SHA256(vendor + event_type + occurred_at + entity_id)`. This handles single-vendor deduplication cleanly (e.g., Samsara sends the same geofence event twice due to retry).

For cross-vendor events (e.g., Samsara and MacroPoint both emit an event about the same truck crossing the same geofence), **both events will be processed**. The deduplication window is 14 days (matching the state store TTL). Cross-vendor deduplication would require a canonical entity ID mapping layer, which is not currently implemented.

**Why not cross-vendor deduplication?**

Cross-vendor deduplication is complex because:
- Different vendors use different entity ID formats (Samsara vehicle ID vs MacroPoint shipment ID)
- Timestamps may have different precision or timezone handling
- Event type semantics differ between vendors

If you need cross-vendor deduplication, implement it in your detector logic by querying the state store for recent events from other vendors with matching semantic criteria.

## Troubleshooting

**Webhook not receiving events**
- Verify Samsara webhook URL is reachable from Samsara's servers
- Check `MANDALA_SAMSARA_WEBHOOK_SECRET` matches Samsara webhook secret
- Check API logs: `docker compose logs api`
- Use ngrok for local testing: `ngrok http 8000`

**Worker not processing events**
- Check worker logs: `docker compose logs worker`
- Verify Redis is healthy: `docker compose ps`
- Check stream has events: `docker compose exec redis redis-cli XLEN mandala:events`

**State store empty**
- Events must be processed by worker before appearing in state store
- Check worker logs for projection errors
- Verify TTL hasn't expired (14-day default)

**MCP server not connecting**
- Verify `mandala[mcp]` is installed: `pip list | grep mandala`
- Check Claude Desktop config JSON syntax
- Test MCP server manually: `mandala mcp` (should wait for stdin)

**Redis memory growing**
- Stream auto-trims at 100,000 messages
- State store keys expire after 14-day TTL
- Monitor with: `docker compose exec redis redis-cli INFO memory`

**Performance issues**
- Add more worker processes: `docker compose up --scale worker=3`
- Add views runner for read-optimized queries: `docker compose up --scale views=1`
- Check Redis CPU/memory: `docker stats mandala-redis-1`
- Consider AWS deployment for production (see Terraform module below)
- Consumer-group lag metrics are published to Prometheus for monitoring

## Four CLI commands. That's it.

```bash
mandala serve     # FastAPI webhook ingest
mandala worker    # event loop: project + alert
mandala views     # materialized views runner (geospatial / timeseries / bitmap / graph)
mandala mcp       # MCP stdio server for LLMs
```

## GitHub Actions

Mandala includes a GitHub Actions workflow for automated daily fleet intelligence reports:

- **Daily Fleet Intelligence Report** — Runs at 6:00 AM UTC daily
- **Report types** — `cross_border_compliance`, `carrier_safety`
- **Output destinations** — Slack webhook, file, or stdout
- **Manual trigger** — Can be run on-demand via `workflow_dispatch`

To enable:

1. Add secrets to your GitHub repo:
   - `SAMSARA_API_KEY` — Your Samsara API token
   - `SLACK_WEBHOOK_URL` — (optional) Slack webhook for notifications

2. Enable the workflow in your repo's Actions tab

3. Run manually or wait for the daily scheduled run

The workflow generates JSON reports with fleet compliance data and uploads artifacts for 30-day retention.

## Terraform Module

For enterprise deployments, Mandala provides an AWS Terraform module in the Terraform Registry:

```hcl
module "mandala" {
  source  = "theoddden/mandala/aws"
  version = "~> 0.1"

  samsara_webhook_secret = var.samsara_key
  vizion_api_key         = var.vizion_key
}
```

**Provisions:**
- AWS ElastiCache Redis (~$15/month)
- Two ECS Fargate tasks (mandala serve + mandala worker)
- Application Load Balancer with HTTPS
- AWS Secrets Manager for API keys
- IAM roles with least-privilege access
- CloudWatch log groups

**Cost:** ~$50-60/month for basic deployment in us-east-1

See [terraform/aws/README.md](terraform/aws/README.md) for full documentation.

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
| `mandala_shipments` | shipment | the single pane of glass |
| `mandala_trucks_current` | truck | latest known truck state |
| `mandala_carrier_safety_profile` | DOT number | live CSA scores, inspection history, FMCSA authority |
| `mandala_intermodal_legs` | container | rail status, ETA, last free day, milestones |
| `mandala_border_crossings` | crossing event | retroactive customs audits |
| `mandala_cold_chain_compliance` | breach window | regulatory liability surface |
| `mandala_carbon_per_trip` | journey | CSRD / CBAM-friendly emissions |

## The schema

Every event is a [CloudEvents 1.0](https://cloudevents.io) envelope with
`type` from the `mandala.*` registry. The full contract — versioned
independently of the codebase — is in **[SCHEMA.md](SCHEMA.md)**.

### Three-Timestamp Event Accounting

Every MandalaEvent includes three timestamps for compliance, audit, and liability tracking:

- **`time`** — When the physical event occurred (e.g., truck crossed POE)
- **`received_at`** — When Mandala's webhook received the event
- **`processed_at`** — When the worker ran detectors on the event

This enables precise detection lag measurement and audit trail reconstruction:

```sql
-- mandala_border_crossings includes these fields
select
    occurred_at,
    received_at,
    processed_at,
    datediff('second', occurred_at, received_at) as detection_lag_sec,
    datediff('second', occurred_at, processed_at) as alert_lag_sec
from mandala_border_crossings
```

For insurance claims and customs disputes, the three timestamps prove when Mandala detected an issue relative to when the event occurred. Schema version bumped to 0.2.

## Risks & privacy

- **[RISKS.md](RISKS.md)** — Descartes API fragmentation, schema breaking
  changes, GDPR exposure, etc.
- **[DATA_PRIVACY.md](DATA_PRIVACY.md)** — Mandala is a connector library,
  not a data store. TTL'd Redis state. No phone-home.

## License

Apache 2.0 — see [LICENSE](LICENSE).

Mandala is not affiliated with Samsara Inc. or The Descartes Systems
Group Inc. References to those products are solely for interoperability
and integration.
