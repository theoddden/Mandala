#!/usr/bin/env python3
"""
Vendor-agnostic mock event generator for Mandala.

Reads any vendor schema YAML and generates realistic mock events
following the canonical Mandala event format.
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Any

import yaml


def load_schema(schema_path: Path) -> dict[str, Any]:
    """Load and parse a vendor schema YAML file."""
    with open(schema_path) as f:
        return yaml.safe_load(f)


def generate_mock_value(field_name: str, context: dict[str, Any]) -> Any:
    """Generate a realistic mock value based on field name and context."""
    field_lower = field_name.lower()

    # IDs
    if "id" in field_lower and "truck" in field_lower:
        return f"TRK-{random.randint(1000, 9999)}"
    if "id" in field_lower and "shipment" in field_lower:
        return f"SHP-{random.randint(100000, 999999)}"
    if "id" in field_lower:
        return str(random.randint(100000, 999999))

    # Names
    if "name" in field_lower:
        return f"Truck {random.randint(1, 100)}"

    # Coordinates
    if "lat" in field_lower:
        return round(random.uniform(25.0, 49.0), 6)  # Continental US bounds
    if "lon" in field_lower:
        return round(random.uniform(-125.0, -66.0), 6)

    # Heading
    if "heading" in field_lower:
        return round(random.uniform(0, 360), 1)

    # Speed
    if "speed" in field_lower:
        return round(random.uniform(0, 75), 1)

    # Timestamps
    if "time" in field_lower or "ts" in field_lower:
        if "base_time" in context:
            base = context["base_time"]
            offset = timedelta(minutes=random.randint(-30, 30))
            return (base + offset).isoformat()
        return datetime.now(UTC).isoformat()

    # Codes
    if "code" in field_lower and "poe" in field_lower:
        return random.choice(["LRD", "BLT", "DET", "BUF", "SEA"])
    if "code" in field_lower:
        return f"CODE-{random.randint(100, 999)}"

    # Types
    if "type" in field_lower and "hold" in field_lower:
        return random.choice(["documentation", "inspection", "regulatory", "security"])
    if "type" in field_lower and "geofence" in field_lower:
        return random.choice(["port_of_entry", "facility", "yard", "customer"])

    # SCAC
    if "scac" in field_lower:
        return random.choice(["MAEU", "HJMX", "CARM", "PITT", "SAIA"])

    # Strings
    if "reason" in field_lower:
        return random.choice([
            "Missing documentation",
            "Inspection required",
            "Regulatory hold",
            "Security screening",
        ])

    # Default: random string
    return f"mock-{random.randint(1000, 9999)}"


def resolve_nested_value(vendor_field: str, example_payload: dict[str, Any]) -> Any:
    """Resolve a nested field from the example payload."""
    parts = vendor_field.split(".")
    value = example_payload
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


def generate_vendor_payload(schema: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Generate a mock vendor payload based on the schema."""
    example = yaml.safe_load(schema["example_vendor_payload"])
    required = schema.get("required_fields", [])
    optional = schema.get("optional_fields", [])

    payload = {}

    # Generate required fields
    for field in required:
        # Try to extract from example first
        value = resolve_nested_value(field, example)
        if value is None:
            value = generate_mock_value(field, context)
        payload[field] = value

    # Generate optional fields (50% chance)
    for field in optional:
        if random.random() > 0.5:
            value = resolve_nested_value(field, example)
            if value is None:
                value = generate_mock_value(field, context)
            payload[field] = value

    return payload


def apply_mapping(
    vendor_payload: dict[str, Any],
    mapping: dict[str, str],
    canonical_type: str,
    vendor: str,
) -> dict[str, Any]:
    """Apply field mapping from vendor to canonical attributes."""
    attributes = {}

    for vendor_field, canonical_attr in mapping.items():
        # Resolve nested vendor field
        parts = vendor_field.split(".")
        value = vendor_payload
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                value = None
                break

        if value is not None:
            attributes[canonical_attr] = value

    # Extract subject from canonical attributes
    subject = None
    if "logistics.truck.id" in attributes:
        subject = f"urn:mandala:truck:{vendor}:{attributes['logistics.truck.id']}"
    elif "logistics.shipment.id" in attributes:
        subject = f"urn:mandala:shipment:{vendor}:{attributes['logistics.shipment.id']}"

    # Extract time
    time_value = attributes.get("time")
    if not time_value:
        time_value = datetime.now(UTC).isoformat()

    return {
        "attributes": attributes,
        "subject": subject,
        "time": time_value,
    }


def build_canonical_event(
    schema: dict[str, Any],
    vendor_payload: dict[str, Any],
    mapped: dict[str, Any],
) -> dict[str, Any]:
    """Build the full canonical Mandala event."""
    import hashlib

    # Generate trace_id from subject
    subject = mapped["subject"]
    trace_id = hashlib.sha256(subject.encode()).hexdigest()[:32]

    # Generate span_id
    span_id = hashlib.sha256(str(random.random()).encode()).hexdigest()[:16]

    return {
        "specversion": "1.0",
        "id": f"{random.randint(10000000, 99999999)}-{random.randint(10000000, 99999999)}",
        "source": f"mandala/connector/{schema['vendor']}",
        "type": schema["canonical_type"],
        "time": mapped["time"],
        "subject": subject,
        "datacontenttype": "application/json",
        "mandalaschemaversion": "0.1",
        "trace_id": trace_id,
        "span_id": span_id,
        "attributes": mapped["attributes"],
        "data": {
            "vendor": schema["vendor"],
            "raw_payload": vendor_payload,
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate mock Mandala events from vendor schema"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        required=True,
        help="Path to vendor schema YAML file",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of mock events to generate (default: 10)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file (default: stdout, JSONL format)",
    )
    parser.add_argument(
        "--vendor-only",
        action="store_true",
        help="Output only vendor payloads, not canonical events",
    )

    args = parser.parse_args()

    # Load schema
    schema = load_schema(args.schema)

    # Generate events
    base_time = datetime.now(UTC)
    context = {"base_time": base_time}

    events = []
    for i in range(args.count):
        # Advance time for each event
        context["base_time"] = base_time + timedelta(seconds=i * 30)

        # Generate vendor payload
        vendor_payload = generate_vendor_payload(schema, context)

        if args.vendor_only:
            events.append(vendor_payload)
        else:
            # Apply mapping
            mapped = apply_mapping(
                vendor_payload,
                schema["mapping"],
                schema["canonical_type"],
                schema["vendor"],
            )

            # Build canonical event
            canonical = build_canonical_event(schema, vendor_payload, mapped)
            events.append(canonical)

    # Output
    if args.output:
        with open(args.output, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        print(f"Generated {len(events)} events to {args.output}", file=sys.stderr)
    else:
        for event in events:
            print(json.dumps(event))


if __name__ == "__main__":
    main()
