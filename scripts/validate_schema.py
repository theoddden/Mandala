#!/usr/bin/env python3
"""
Schema validation utility for Mandala vendor schemas.

Validates vendor schema YAML files and optionally validates real vendor payloads
against the schema.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml


def load_schema(schema_path: Path) -> dict[str, Any]:
    """Load and parse a vendor schema YAML file."""
    with open(schema_path) as f:
        return yaml.safe_load(f)


def validate_schema_structure(schema: dict[str, Any]) -> list[str]:
    """Validate the schema YAML structure."""
    errors = []

    required_fields = [
        "vendor",
        "canonical_type",
        "description",
        "mapping",
        "example_vendor_payload",
        "required_fields",
    ]

    for field in required_fields:
        if field not in schema:
            errors.append(f"Missing required field: {field}")

    # Validate mapping is a dict
    if "mapping" in schema and not isinstance(schema["mapping"], dict):
        errors.append("mapping must be a dictionary")

    # Validate required_fields is a list
    if "required_fields" in schema and not isinstance(schema["required_fields"], list):
        errors.append("required_fields must be a list")

    # Validate optional_fields is a list if present
    if "optional_fields" in schema and not isinstance(schema["optional_fields"], list):
        errors.append("optional_fields must be a list")

    # Validate example_vendor_payload is valid JSON
    if "example_vendor_payload" in schema:
        try:
            yaml.safe_load(schema["example_vendor_payload"])
        except yaml.YAMLError as e:
            errors.append(f"example_vendor_payload is not valid YAML/JSON: {e}")

    return errors


def validate_payload_against_schema(
    payload: dict[str, Any], schema: dict[str, Any]
) -> list[str]:
    """Validate a vendor payload against the schema."""
    errors = []

    required = schema.get("required_fields", [])

    for field in required:
        # Check nested fields
        parts = field.split(".")
        value = payload
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                errors.append(f"Missing required field: {field}")
                break

    # Apply validation rules if present
    validation_rules = schema.get("validation", [])
    for rule in validation_rules:
        if not apply_validation_rule(payload, rule):
            errors.append(f"Validation rule failed: {rule}")

    return errors


def apply_validation_rule(payload: dict[str, Any], rule: str) -> bool:
    """Apply a single validation rule to the payload."""
    # Simple rule parser - can be extended
    if "must be between" in rule:
        # Extract field name and bounds
        parts = rule.split()
        field_idx = parts.index("must") - 1
        field_name = parts[field_idx]
        between_idx = parts.index("between")
        min_val = float(parts[between_idx + 1])
        max_val = float(parts[between_idx + 3].rstrip(","))

        # Resolve nested field
        field_parts = field_name.split(".")
        value = payload
        for part in field_parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return True  # Field not present, skip validation

        if isinstance(value, (int, float)):
            return min_val <= value <= max_val

    if "must be one of" in rule:
        # Extract field name and allowed values
        parts = rule.split()
        field_idx = parts.index("must") - 1
        field_name = parts[field_idx]
        of_idx = parts.index("of")
        allowed_values = parts[of_idx + 1 :]

        # Resolve nested field
        field_parts = field_name.split(".")
        value = payload
        for part in field_parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return True  # Field not present, skip validation

        return str(value) in allowed_values

    if "must be valid POE code" in rule:
        # Extract field name
        parts = rule.split()
        field_idx = parts.index("must") - 1
        field_name = parts[field_idx]

        # Resolve nested field
        field_parts = field_name.split(".")
        value = payload
        for part in field_parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return True  # Field not present, skip validation

        if isinstance(value, str):
            return 3 <= len(value) <= 4

    # Default: pass if rule not recognized
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Validate Mandala vendor schemas and payloads"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        required=True,
        help="Path to vendor schema YAML file",
    )
    parser.add_argument(
        "--payload",
        type=Path,
        help="Path to vendor payload JSON file to validate against schema",
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only validate schema structure, not payloads",
    )

    args = parser.parse_args()

    # Load schema
    schema = load_schema(args.schema)

    # Validate schema structure
    schema_errors = validate_schema_structure(schema)
    if schema_errors:
        print("Schema validation errors:", file=sys.stderr)
        for error in schema_errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    print(f"✓ Schema structure valid: {args.schema}", file=sys.stderr)

    # Validate payload if provided
    if args.payload and not args.schema_only:
        with open(args.payload) as f:
            payload = json.load(f)

        payload_errors = validate_payload_against_schema(payload, schema)
        if payload_errors:
            print("Payload validation errors:", file=sys.stderr)
            for error in payload_errors:
                print(f"  - {error}", file=sys.stderr)
            sys.exit(1)

        print(f"✓ Payload valid against schema", file=sys.stderr)

    print("✓ All validations passed", file=sys.stderr)


if __name__ == "__main__":
    main()
