# Mandala Canonical Schema

**Version 0.1** — versioned independently of the codebase.

This document is the public contract for the Mandala canonical event
schema. Anything that produces or consumes Mandala events — the Python
library, the `dbt-mandala` package, the MCP server, third-party
integrations — must conform to this spec. Implementations may be
ahead of or behind the spec; the version field on every event tells
consumers which contract was in force at production time.

## Versioning

Mandala uses semantic versioning **of the schema itself**, separate from
library versions:

| Schema bump | Trigger |
|---|---|
| Patch (`0.1.0 → 0.1.1`) | Documentation fixes, additional optional fields. |
| Minor (`0.1 → 0.2`) | New event types, new optional schema fields, additive enum values. |
| Major (`0.1 → 1.0`) | Removed or renamed fields, removed event types, breaking enum changes. |

Every event carries the schema version it was produced under, in the
CloudEvents extension attribute `mandalaschemaversion`. Consumers must
either pin a single major version or implement explicit migration logic.

The current spec is **0.1** (alpha). Treat it as unstable until the 1.0
declaration; bumps will come with migration notes.

## Envelope

Every fact about the world in Mandala is wrapped in a [CloudEvents 1.0]
(https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
envelope. JSON serialization is the canonical wire format.

| Attribute | Type | Required | Notes |
|---|---|:-:|---|
| `specversion` | string | yes | Always `"1.0"`. |
| `id` | string | yes | Globally unique per delivered event. UUIDs recommended. |
| `source` | URI-ref | yes | Producer identifier, e.g. `mandala/connector/samsara`. |
| `type` | string | yes | One of the values in *Event Types* below, or a future value. |
| `time` | RFC 3339 | yes | When the producer **observed** the fact. |
| `subject` | string | recommended | A Mandala URN (see *Identifiers*). |
| `datacontenttype` | string | no | Defaults to `application/json`. |
| `dataschema` | URI | no | Reserved for future per-type JSON Schemas. |
| `data` | object | yes | Payload — shape depends on `type`. |
| `mandalaschemaversion` | string | yes | Mandala schema version, e.g. `"0.1"`. |
| `mandalaingestid` | string | no | Idempotency key from the raw inbound webhook. |
| `traceparent` | string | no | W3C Trace Context. |

## Identifiers

Mandala URNs identify entities canonically:

```
urn:mandala:<entity>:<scope>:<id>
```

| Entity | Scope examples | Example |
|---|---|---|
| `truck` | `samsara`, `geotab`, `motive` | `urn:mandala:truck:samsara:281474976710656` |
| `shipment` | `macropoint`, `descartes`, `internal` | `urn:mandala:shipment:macropoint:DES-2026-001234` |
| `bol` | `descartes`, `internal` | `urn:mandala:bol:descartes:BOL-987654` |
| `customs-entry` | `cbp`, `cbsa`, `sat` | `urn:mandala:customs-entry:cbp:316-1234567-9` |
| `party` | scope is the originating system | `urn:mandala:party:samsara-driver:42` |

URN parts are case-sensitive. The `id` segment may contain `:` characters;
parsers must split on the first three colons only.

## Event Types

All types live in the `mandala.*` namespace. Adding a type is a minor
schema bump; removing one is a major bump. Unknown types must be
forwarded unchanged, never rejected.

### Truck telemetry

| Type | Subject | `data` |
|---|---|---|
| `mandala.truck.position.updated` | `truck` URN | `TruckTelemetry` (truck + position) |
| `mandala.truck.geofence.entered` | `truck` URN | `{ truck_id, geofence_id, geofence_name, occurred_at, vendor }` |
| `mandala.truck.geofence.exited` | `truck` URN | same as above |
| `mandala.truck.eta.updated` | `truck` URN | `{ truck_id, eta, source }` |
| `mandala.truck.harsh_event.detected` | `truck` URN | `{ truck_id, behavior, g_force, occurred_at }` |
| `mandala.truck.fuel.low` | `truck` URN | `{ truck_id, fuel_pct, threshold }` |
| `mandala.truck.door.opened` | `truck` URN | `{ truck_id, door_id, occurred_at }` |

### Cold chain

| Type | Subject | `data` |
|---|---|---|
| `mandala.truck.cold_chain.reading` | `truck` URN | `ColdChainReading` |
| `mandala.truck.cold_chain.breach` | `truck` URN | `ColdChainReading` |
| `mandala.truck.cold_chain.recovered` | `truck` URN | `ColdChainReading` |

### Driver / HOS

| Type | Subject | `data` |
|---|---|---|
| `mandala.driver.assigned` | `party` URN | `Driver` |
| `mandala.driver.hos.warning` | `party` URN | `{ driver_id, remaining_drive_minutes }` |
| `mandala.driver.hos.violation` | `party` URN | `{ driver_id, violation_type, occurred_at }` |

### Shipment lifecycle

| Type | Subject | `data` includes |
|---|---|---|
| `mandala.shipment.booked` | `shipment` URN | order_number, carrier_scac, origin, destination |
| `mandala.shipment.dispatched` | `shipment` URN | status timestamp |
| `mandala.shipment.picked_up` | `shipment` URN | location |
| `mandala.shipment.in_transit` | `shipment` URN | location, eta |
| `mandala.shipment.at_border` | `shipment` URN | border_poe |
| `mandala.shipment.delivered` | `shipment` URN | location, signed_by |
| `mandala.shipment.cancelled` | `shipment` URN | reason |
| `mandala.shipment.eta.updated` | `shipment` URN | eta, source |
| `mandala.shipment.handoff.confirmed` | `shipment` URN | truck_urn, shipment_urn |

### Customs

| Type | Subject | `data` includes |
|---|---|---|
| `mandala.shipment.customs.filed` | `shipment` URN | authority, entry_number, importer, broker |
| `mandala.shipment.customs.hold` | `shipment` URN | authority, hold_reason |
| `mandala.shipment.customs.exam` | `shipment` URN | authority, exam_type |
| `mandala.shipment.customs.released` | `shipment` URN | authority, released_at |
| `mandala.shipment.customs.rejected` | `shipment` URN | authority, reason |

### Bills of lading

| Type | Subject | `data` |
|---|---|---|
| `mandala.shipment.bol.received` | `shipment` URN | `BillOfLading` |
| `mandala.shipment.bol.amended` | `shipment` URN | `BillOfLading` plus `amended_fields` |

### Compliance

| Type | Subject | `data` |
|---|---|---|
| `mandala.party.screened.clear` | `party` URN | `{ party, list_versions }` |
| `mandala.party.screened.flagged` | `party` URN | `{ party, list_name, match_score }` |

### Trade intelligence

| Type | Subject | `data` |
|---|---|---|
| `mandala.trade.lane.insight` | n/a | `{ origin_country, dest_country, hs_code, period, value_usd, weight_kg }` |

### Capacity / load board

| Type | Subject | `data` |
|---|---|---|
| `mandala.truck.empty` | `truck` URN | `{ truck_urn, shipment_urn, delivered_at, last_position, equipment, vin, license_plate }` |
| `mandala.truck.available` | `truck` URN | `{ truck_urn, available_at, last_position, equipment, hos_remaining_min }` |
| `mandala.loadboard.posted` | `truck` URN | `{ truck_urn, board, posting_id, equipment, origin, ttl_hours, radius_mi, external_reference }` |
| `mandala.loadboard.post_failed` | `truck` URN | `{ truck_urn, board, error, reason? }` |
| `mandala.loadboard.expired` | `truck` URN | `{ truck_urn, board, posting_id }` |

### Alerts (Mandala-internal, derived)

| Type | Subject | `data` |
|---|---|---|
| `mandala.alert.cross_border.no_filing` | `truck` URN | `{ truck_urn, shipment_urn?, border_poe, customs_status?, reason, severity }` |
| `mandala.alert.cross_border.hold` | `truck` URN | `{ truck_urn, shipment_urn, border_poe, severity }` |
| `mandala.alert.cold_chain.out_of_spec` | `truck` URN | `{ truck_urn, shipment_urn?, temperature_c, declared_min_c, declared_max_c, severity }` |

## Canonical objects

The `data` payloads above reference the following nested objects. The
authoritative Pydantic models live at
`src/mandala/core/schema/`; this is the prose summary.

### `Shipment`

```jsonc
{
  "id": "DES-2026-001234",
  "reference": "PO-887",
  "status": "in_transit",                  // ShipmentStatus
  "customs_status": "filed",                // CustomsStatus
  "shipper":   { "id": "...", "name": "...", "role": "shipper",   "address": {...} },
  "consignee": { "id": "...", "name": "...", "role": "consignee", "address": {...} },
  "carrier":   { "id": "...", "name": "...", "role": "carrier" },
  "broker":    { "id": "...", "name": "...", "role": "broker" },
  "legs": [ /* ShipmentLeg[] */ ],
  "commodities": [ /* CommodityLine[] */ ],
  "customs_entry": { /* CustomsEntry */ },
  "bills_of_lading": [ /* BillOfLading[] */ ],
  "eta": "2026-05-09T18:00:00Z",
  "eta_confidence": 0.78,
  "last_position": { "lat": 27.50, "lon": -99.51 },
  "cold_chain_required": true,
  "cold_chain_min_c": 2.0,
  "cold_chain_max_c": 8.0,
  "metadata": {},
  "updated_at": "2026-05-08T22:14:09Z"
}
```

### Enums

| Enum | Values |
|---|---|
| `ShipmentStatus` | `booked`, `dispatched`, `in_transit`, `at_border`, `held`, `delivered`, `cancelled` |
| `CustomsStatus`  | `not_filed`, `filed`, `under_review`, `hold`, `exam`, `released`, `rejected` |
| `TransportMode`  | `truck`, `rail`, `ocean`, `air`, `intermodal` |
| `HazmatClass`    | `none`, `1`–`9` |

State machine — `CustomsStatus`:
```
not_filed → filed → { under_review | hold | exam | released | rejected }
under_review → { released | hold | rejected }
hold → { released | rejected }
exam → { released | rejected }
```

### `Truck`, `TruckPosition`, `ColdChainReading`, `Party`, `BillOfLading`,
`CustomsEntry`, `CommodityLine`, `Geofence`, `BorderCrossing`

See `src/mandala/core/schema/` for field-level definitions. Every model is a
Pydantic `BaseModel`; the JSON serialization is the canonical wire form.

## Cross-references

- `dbt-mandala` consumes events conforming to this schema from the
  `raw_mandala_events` warehouse table. Each Mandala schema minor version
  corresponds to a dbt-mandala minor version.
- The Python library version exporting the schema can be checked at
  runtime: `mandala.core.events.envelope.SCHEMA_VERSION`.
- The MCP server tools return the same canonical objects, JSON-serialized.

## Compatibility promise

For schema 0.x: best-effort compatibility, breaking changes flagged in
release notes. For schema 1.x and above: strict semver, deprecations
announced one minor version ahead of removal.
