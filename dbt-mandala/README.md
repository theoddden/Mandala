# dbt-mandala

> Warehouse-native canonical schema for Samsara fleet telemetry and Descartes trade data — the dbt distribution of the [Mandala](https://github.com/mandala-bridge/mandala) event bridge.

`dbt deps` and you have normalized, tested, documented `mandala_shipments`, `mandala_trucks_current`, `mandala_border_crossings`, `mandala_cold_chain_compliance`, and `mandala_carbon_per_trip` materializing in your warehouse. Every downstream model, dashboard, and AI agent in your stack inherits the [canonical Mandala schema](https://github.com/mandala-bridge/mandala/blob/main/SCHEMA.md) automatically.

## Install

In your `packages.yml`:

```yaml
packages:
  - package: mandala-bridge/mandala
    version: [">=0.1.0", "<0.2.0"]
```

Then:

```bash
dbt deps
```

## Required source

`dbt-mandala` consumes a single source table populated by the Mandala warehouse sink (or by your own pipeline writing CloudEvents JSON). The default name is `raw_mandala_events`:

| column | type | description |
|---|---|---|
| `event_id` | string | CloudEvents `id` |
| `event_type` | string | e.g. `mandala.shipment.customs.filed` |
| `source` | string | producer URI (e.g. `mandala/connector/samsara`) |
| `subject` | string | Mandala URN (e.g. `urn:mandala:shipment:macropoint:DES-001`) |
| `event_time` | timestamp | producer-observed time |
| `ingested_at` | timestamp | warehouse arrival time |
| `schema_version` | string | Mandala schema version (e.g. `0.1`) |
| `payload` | variant / json / super | CloudEvents `data` payload |

Configure the source name and schema in `dbt_project.yml`:

```yaml
vars:
  mandala:
    raw_database: ANALYTICS
    raw_schema: RAW
    raw_table: raw_mandala_events
```

## What you get

### Staging (1:1 with the canonical schema)

- `stg_mandala__events` — parsed CloudEvents envelope (one row per ingested event).
- `stg_mandala__truck_positions`
- `stg_mandala__shipment_milestones`
- `stg_mandala__customs_entries`
- `stg_mandala__cold_chain_readings`
- `stg_mandala__geofence_crossings`
- `stg_mandala__alerts`

### Intermediate

- `int_mandala__truck_journeys` — sessionised truck position runs.
- `int_mandala__shipment_timeline` — full lifecycle per shipment.
- `int_mandala__cold_chain_breaches` — temperature-out-of-range windows.

### Marts (the consumable layer)

- `mandala_shipments` — **one row per shipment** with status, customs status, ETA, carrier, broker, latest position, and timeline.
- `mandala_trucks_current` — latest known state per truck.
- `mandala_border_crossings` — ledger of every Port-of-Entry geofence crossing, joined to customs filing status.
- `mandala_cold_chain_compliance` — temperature breaches matched to declared shipment requirements.
- `mandala_carbon_per_trip` — measured CO₂ per trip leg from fuel-consumption telemetry.

## Supported warehouses

- Snowflake (full)
- BigQuery (full)
- Postgres / Redshift (full)
- Databricks (full)
- DuckDB (for local dev / CI)

JSON access is wrapped in macros (`mandala_json_get`) so warehouse differences are abstracted.

## Tests

The package ships with `dbt-expectations`-compatible tests on every staging model: schema invariants, URN format, monotonically-increasing event time, no-orphan-references between trucks and shipments, and customs-status state-machine validity.

## License

Apache 2.0.
