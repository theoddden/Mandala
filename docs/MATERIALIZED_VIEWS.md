# Materialized Views

Mandala applies **CQRS** (Command/Query Responsibility Segregation) on top of
Redis Streams. The stream `mandala:events` is the authoritative, append-only
**write model**. Every module in `mandala.views` is a **read model** — a
specialized data structure optimized for one query pattern, continuously
maintained by subscribing to the stream via a dedicated consumer group.

This lets the worker stay fast and focused on projection + detectors while
the views absorb expensive query shapes that would otherwise require
state-store scans or external analytics stores.

## Why CQRS here?

The canonical state store (`mandala.core.state.StateStore`) is a flat Redis
KV projection: given a URN, you can fetch the current state of a shipment
or truck in O(1). That's sufficient for single-entity lookups (the MCP
`get_truck` / `get_shipment` tools) but quickly breaks down for:

- **Spatial queries** — "trucks within 50 mi of Laredo": scan all trucks,
  read each one, Haversine-filter. O(N) per query.
- **Set-algebra queries** — "trucks at POE without a released customs
  filing": join presence × filing status, which requires either a JOIN in
  a SQL warehouse or a multi-key scan in Redis.
- **Temporal queries** — "all cold-chain breaches in the last 24h": state
  store only keeps the latest reading; history lives on the stream or the
  warehouse.
- **Graph queries** — "which shipper's goods have passed through Laredo
  and had a customs hold in the last 30 days?": multi-hop traversal.

Materialized views solve each of these with a purpose-built Redis
structure.

## Architecture

```
                 ┌────────────────────────────────────┐
   webhooks ─▶   │   Redis Stream: mandala:events     │
                 └───────────────┬────────────────────┘
                                 │
         ┌───────────────────────┼────────────────────────┐
         ▼                       ▼                        ▼
  consumer group           consumer group           consumer group
   "mandala"               "mandala:views"            "mandala:sink"
   (projection              (this package)              (warehouse)
    + detectors)
```

Each consumer group has its own pending-entries list and its own `XACK`
cadence, so a slow view never blocks the worker's detectors and vice
versa. Views are fanned out within the runner via `asyncio.gather(...,
return_exceptions=True)` — one view crashing does not affect the others.

Run with:

```bash
mandala views
```

Enable per view via settings (`MANDALA_VIEWS_*` env vars).

## Design contract

Every view implements `mandala.views.base.MaterializedView` and must be:

1. **Idempotent** — Redis Streams + consumer groups provide at-least-once
   delivery. `apply(event)` must produce the same state when called twice
   with the same event. In practice: use `GEOADD` (overwrites), `ZADD` on
   `(score, member)` (no-op if unchanged), `SETBIT` (idempotent by
   definition), `MERGE` in Cypher (idempotent).
2. **Monotonic** — later events override earlier state for the same key.
3. **Read-only against the StateStore** — views may read from state (e.g.
   bitmap view reads `shipment_for_truck`) but must never write to it.
4. **Rebuildable** — given an empty Redis and the full event history
   replayed from `id=0`, the view's final state must match the live one.

## The views

### `GeospatialView` — `mandala:view:geo:trucks`

Subscribes to `mandala.truck.position.updated`. Maintains a Redis GEO set
keyed by truck URN. Query surface:

- `trucks_near(lat, lon, radius_mi, limit)` → `GEOSEARCH`.
- `truck_position(urn)` → `GEOPOS`.

Rebuild: replay the stream; last `GEOADD` for each URN wins.

### `TimeseriesView` — `mandala:view:ts:cold:*`

Subscribes to `mandala.truck.cold_chain.*`. Two data structures:

- Per-truck sorted set of temperature readings (score = epoch seconds).
- Global breach index sorted set for fleet-wide queries.

Retention is bounded by `views_timeseries_ttl_seconds` (default 7 days);
each `apply` trims entries older than `now - ttl` via `ZREMRANGEBYSCORE`.

### `BitmapView` — `mandala:view:bm:poe:*`

Two bitmaps per Port-of-Entry:

- `…:present` — bit = 1 while the truck is inside the POE geofence.
- `…:filed`   — bit = 1 while the truck's linked shipment has a filing in
  `{filed, released}`.

Answer "at POE without filing" with `BITOP NOT` + `BITOP AND` — pure
boolean set algebra in O(bitmap-size / 8). Truck URN → integer offset
mapping is managed by a tiny Lua script (`GET_OR_CREATE_ID_SCRIPT`) so
offsets are stable across restarts and reusable across all POE bitmaps.

### `GraphView` — RedisGraph / FalkorDB

Optional; requires the RedisGraph (deprecated) or
[FalkorDB](https://www.falkordb.com/) module. Projects:

- `(Truck)-[:HAULS]->(Shipment)` from `SHIPMENT_HANDOFF`.
- `(Shipment)-[:FILED_WITH]->(Authority)` from `CUSTOMS_FILED`.
- `(Truck)-[:CROSSED]->(POE)` from `TRUCK_GEOFENCE_ENTERED`.

On startup the view probes `MODULE LIST`; if neither module is loaded it
logs a single warning and becomes a no-op. This keeps graph optional
without a hard dependency.

## Consistency model

- **Eventually consistent** — a view lags the main worker by exactly the
  latency of one `XREADGROUP` cycle plus the view's own `apply` time
  (typically sub-10 ms each).
- **No cross-view transactions** — if geospatial and bitmap views diverge
  mid-fan-out, the next events eventually converge them. Queries that
  need both views to agree (e.g. "trucks at POE within 10 mi of Laredo")
  should tolerate a small window of disagreement.
- **Cold start** — on first run the views are empty. Either:
  1. Rebuild by replaying from `id=0`: `mandala views` with a fresh
     consumer group (not yet wired as a CLI flag; manual `XGROUP CREATE`
     with `id=0` required).
  2. Start with an empty view and let it fill from new events. This is
     the default; the MCP tools that query the views fall back to the
     state-store scan when the view is empty (see
     `tool_get_fleet_near_border`).

## Failure modes

- **View crashes on apply** — `runner.py` catches exceptions per-view and
  continues. The event is still acked so the view group does not back up.
  Failures are logged + counted via `mandala_view_apply_total{status="failure"}`.
- **View drifts from reality** — rebuild from `id=0`. Because views are
  idempotent, replay is safe.
- **Redis Stream pruned** — with the default `maxlen=100_000` on publish,
  a cold view started after major volume might miss events outside the
  retention window. Mitigation: keep the warehouse sink's JSONL archive
  as the long-term replay source; for views, replay from the most recent
  100k events is typically sufficient.

## MCP surface

The new view-backed tools registered on the MCP server:

| Tool                                  | Backing view    | Query shape                                         |
| ------------------------------------- | --------------- | --------------------------------------------------- |
| `get_fleet_near_border`               | Geospatial      | `GEOSEARCH` (falls back to state scan if empty)     |
| `get_trucks_at_poe_without_filing`    | Bitmap          | `BITOP NOT` + `BITOP AND`                           |
| `get_cold_chain_breaches`             | Timeseries      | `ZRANGEBYSCORE` on global breach index              |
| `get_entity_neighbors`                | Graph           | `MATCH (n)-[*1..d]-(m)` via RedisGraph / FalkorDB   |

Each tool is a thin `async def` in `mandala.mcp.server`; adding more is
one function + one `Tool(...)` entry.

## Metrics

```
mandala_view_apply_total{view,status}                  # counter
mandala_view_apply_duration_seconds{view}              # histogram
```

Enable with `MANDALA_METRICS_ENABLED=true` on the `mandala views` process.
