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
| `core/state.py` | Redis-backed projection with TTL (14-day default) | 65 |

**The pattern:**
1. **Ingest** — webhook receives vendor payload → normalize to `MandalaEvent` → publish to Redis Stream
2. **Process** — single worker reads stream → projects into `StateStore` → runs detectors → publishes alerts back to stream
3. **Query** — MCP server reads from `StateStore` (read-only, no writes)

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
  delivery confirmation lands, Mandala emits `mandala.truck.empty` and
  posts available capacity to every configured board with the truck's
  current GPS position and equipment type. Disabled by default
  (`MANDALA_LOADBOARD_ENABLED=0`); requires partner credentials per board.
- **MCP server** — five read-only tools (`get_shipment`, `get_truck`,
  `check_customs_status`, `get_recent_alerts`, `get_fleet_near_border`).
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

```bash
git clone https://github.com/theoddden/Mandala
cd Mandala
cp .env.example .env             # set MANDALA_SAMSARA_WEBHOOK_SECRET, etc.
docker compose up -d             # redis + api + worker
```

Point your Samsara webhook at `http://YOUR_HOST:8000/webhooks/samsara`.
You'll see normalized `MandalaEvent` JSON on the `mandala:events` Redis
stream within seconds.

To talk to Mandala from an LLM:

```bash
mandala mcp                       # stdio MCP server
```

Add it to your Claude Desktop or Continue config under `mcpServers`.

## Three CLI commands. That's it.

```bash
mandala serve     # FastAPI webhook ingest
mandala worker    # event loop: project + alert
mandala mcp       # MCP stdio server for LLMs
```

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
