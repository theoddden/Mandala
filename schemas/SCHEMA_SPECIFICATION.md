# Mandala Schema Specification

**Version 1.0** — Defines how to map any vendor's data format to Mandala canonical events.

## Purpose

Mandala is a neutral event bridge. We don't dictate which vendors you integrate. This specification defines the contract for mapping **any** vendor's webhook/polling data to Mandala's canonical `mandala.*` event types.

## Schema File Format

Each vendor integration is defined by a YAML file in your `schemas/` directory:

```yaml
# schemas/my-vendor/my-event-type.yaml

vendor: my-vendor
canonical_type: mandala.truck.position.updated
description: Human-readable description

mapping:
  # vendor_field: mandala_canonical_attribute
  truckId: logistics.truck.id
  gpsLat: logistics.location.latlon.latitude
  gpsLon: logistics.location.latlon.longitude
  eventTime: time

example_vendor_payload: |
  {
    "truckId": "TRK-001",
    "gpsLat": 34.0522,
    "gpsLon": -118.2437,
    "eventTime": "2026-05-11T20:30:00Z"
  }

required_fields:
  - truckId
  - gpsLat
  - gpsLon
  - eventTime

optional_fields:
  - heading
  - speed
```

## Schema Fields

### Required Top-Level Fields

| Field | Type | Description |
|---|---|---|
| `vendor` | string | Vendor identifier (e.g., `samsara`, `descartes`, `my-custom-system`) |
| `canonical_type` | string | Mandala event type from SCHEMA.md (e.g., `mandala.truck.position.updated`) |
| `description` | string | Human-readable description of this event |
| `mapping` | object | Field mappings from vendor → canonical |
| `example_vendor_payload` | string | Example raw vendor payload (JSON string) |
| `required_fields` | array | Fields that must be present in vendor payload |

### Optional Top-Level Fields

| Field | Type | Description |
|---|---|---|
| `optional_fields` | array | Fields that may be present in vendor payload |
| `validation` | object | Custom validation rules (see below) |

## Mapping Syntax

The `mapping` object defines how vendor fields map to Mandala's `logistics.*` semantic conventions:

```yaml
mapping:
  vendorField: logistics.canonical.attribute
  nested.field: logistics.nested.attribute
```

**Rules:**
- Keys are vendor field names (supports dot notation for nested fields)
- Values are Mandala canonical attribute names from `logistics.*` namespace
- If a vendor field has no direct mapping, omit it from the mapping

## Canonical Attributes

Mandala uses the `logistics.*` semantic convention namespace. Common attributes:

| Attribute | Description |
|---|---|
| `logistics.truck.id` | Truck identifier |
| `logistics.truck.name` | Truck name/label |
| `logistics.location.latlon.latitude` | Latitude coordinate |
| `logistics.location.latlon.longitude` | Longitude coordinate |
| `logistics.location.geofence.id` | Geofence identifier |
| `logistics.location.geofence.name` | Geofence name |
| `logistics.location.poe.code` | Port of Entry code |
| `logistics.shipment.id` | Shipment identifier |
| `logistics.shipment.number` | Shipment number |
| `logistics.carrier.scac` | Carrier SCAC code |
| `logistics.compliance.hold.type` | Hold type |
| `logistics.compliance.hold.reason` | Hold reason |
| `time` | Event timestamp (always maps to canonical `time` field) |

See `SCHEMA.md` for the full canonical attribute registry.

## Validation Rules

Optional `validation` object defines custom validation logic:

```yaml
validation:
  - latitude must be between -90 and 90
  - longitude must be between -180 and 180
  - holdType must be one of: documentation, inspection, regulatory
  - portOfEntry must be valid POE code (3-4 characters)
```

Validation is applied when:
- Generating mock events
- Validating real vendor payloads (if using schema validation middleware)

## Example: Custom Fleet System

```yaml
# schemas/my-fleet/truck-position.yaml

vendor: my-fleet
canonical_type: mandala.truck.position.updated
description: Real-time truck position from custom fleet system

mapping:
  vehicle_id: logistics.truck.id
  vehicle_name: logistics.truck.name
  lat: logistics.location.latlon.latitude
  lon: logistics.location.latlon.longitude
  heading_deg: logistics.location.heading
  speed_mph: logistics.location.speed_mph
  ts: time

example_vendor_payload: |
  {
    "vehicle_id": "TRK-001",
    "vehicle_name": "Truck 42",
    "lat": 34.0522,
    "lon": -118.2437,
    "heading_deg": 270.0,
    "speed_mph": 55.0,
    "ts": "2026-05-11T20:30:00Z"
  }

required_fields:
  - vehicle_id
  - lat
  - lon
  - ts

optional_fields:
  - vehicle_name
  - heading_deg
  - speed_mph

validation:
  - lat must be between -90 and 90
  - lon must be between -180 and 180
  - heading_deg must be between 0 and 360 (if present)
  - speed_mph must be non-negative (if present)
```

## Usage

Once you've defined your schema:

```bash
# Generate mock events
python scripts/generate_mock_events.py --schema schemas/my-fleet/truck-position.yaml --count 100

# Validate real vendor payloads
python scripts/validate_schema.py --schema schemas/my-fleet/truck-position.yaml --payload real_payload.json

# Convert vendor payload to canonical event
python scripts/convert_payload.py --schema schemas/my-fleet/truck-position.yaml --payload real_payload.json
```

## Philosophy

**Mandala is the stator.** We don't dictate which vendors you integrate. We provide the framework for ANY vendor to be processed through the canonical event bridge.

You define the mapping. We handle the projection, detection, alerting, and OTLP export.
