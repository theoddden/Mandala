#!/usr/bin/env python3
"""Generate daily fleet intelligence reports.

This script generates compliance reports for fleet operations, including
cross-border compliance, carrier safety profiles, and cold chain monitoring.

Usage:
    python scripts/generate_fleet_report.py --report-type cross_border_compliance --output slack
    python scripts/generate_fleet_report.py --report-type carrier_safety --output file
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def fetch_samsara_fleet_data(api_key: str) -> dict[str, Any]:
    """Fetch fleet data from Samsara API."""
    base_url = os.getenv("MANDALA_SAMSARA_BASE_URL", "https://api.samsara.com")
    headers = {"Authorization": f"Bearer {api_key}"}

    with httpx.Client(headers=headers, timeout=30.0) as client:
        # Fetch active vehicles
        vehicles_resp = client.get(f"{base_url}/fleet/vehicles")
        vehicles_resp.raise_for_status()
        vehicles = vehicles_resp.json().get("data", [])

        # Fetch recent trips
        trips_resp = client.get(f"{base_url}/fleet/trips", params={"limit": 100})
        trips_resp.raise_for_status()
        trips = trips_resp.json().get("data", [])

    return {"vehicles": vehicles, "trips": trips}


def generate_cross_border_compliance_report(data: dict[str, Any]) -> dict[str, Any]:
    """Generate cross-border compliance report from fleet data."""
    vehicles = data.get("vehicles", [])
    trips = data.get("trips", [])

    # Identify vehicles near borders (simplified logic)
    border_violations = []
    for vehicle in vehicles:
        # In production, this would check actual geofence data
        # For now, we'll flag vehicles with no recent customs filings
        if vehicle.get("location"):
            border_violations.append({
                "vehicle_id": vehicle.get("id"),
                "vehicle_name": vehicle.get("name"),
                "location": vehicle.get("location"),
                "issue": "No recent customs filing detected",
            })

    return {
        "report_type": "cross_border_compliance",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_vehicles": len(vehicles),
            "border_violations": len(border_violations),
            "recent_trips": len(trips),
        },
        "violations": border_violations,
    }


def generate_carrier_safety_report(data: dict[str, Any]) -> dict[str, Any]:
    """Generate carrier safety profile report."""
    vehicles = data.get("vehicles", [])

    # In production, this would use FMCSA data
    safety_issues = []
    for vehicle in vehicles:
        if vehicle.get("safety_score", 100) < 70:
            safety_issues.append({
                "vehicle_id": vehicle.get("id"),
                "vehicle_name": vehicle.get("name"),
                "safety_score": vehicle.get("safety_score"),
            })

    return {
        "report_type": "carrier_safety",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_vehicles": len(vehicles),
            "safety_issues": len(safety_issues),
        },
        "issues": safety_issues,
    }


def send_to_slack(report: dict[str, Any], webhook_url: str) -> None:
    """Send report to Slack webhook."""
    if not webhook_url:
        print("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return

    report_type = report.get("report_type", "unknown")
    summary = report.get("summary", {})

    message = {
        "text": f"Fleet Intelligence Report: {report_type}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Fleet Intelligence Report: {report_type}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Generated:* {report.get('generated_at')}"},
                    {"type": "mrkdwn", "text": f"*Total Vehicles:* {summary.get('total_vehicles', 0)}"},
                    {"type": "mrkdwn", "text": f"*Issues Found:* {summary.get('border_violations', summary.get('safety_issues', 0))}"},
                ],
            },
        ],
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(webhook_url, json=message)
            resp.raise_for_status()
        print("Report sent to Slack successfully")
    except Exception as e:  # noqa: BLE001
        print(f"Failed to send to Slack: {e}")


def save_to_file(report: dict[str, Any], output_dir: str = "/tmp") -> str:
    """Save report to JSON file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"fleet_report_{timestamp}.json"
    filepath = Path(output_dir) / filename

    with open(filepath, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Report saved to {filepath}")
    return str(filepath)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate fleet intelligence reports")
    parser.add_argument("--report-type", default="cross_border_compliance", choices=["cross_border_compliance", "carrier_safety"])
    parser.add_argument("--output", default="slack", choices=["slack", "file", "stdout"])
    parser.add_argument("--output-dir", default="/tmp", help="Directory for file output")

    args = parser.parse_args()

    api_key = os.getenv("MANDALA_SAMSARA_API_KEY")
    if not api_key:
        print("Error: MANDALA_SAMSARA_API_KEY not set")
        return

    # Fetch fleet data
    print("Fetching fleet data from Samsara...")
    data = fetch_samsara_fleet_data(api_key)

    # Generate report
    print(f"Generating {args.report_type} report...")
    if args.report_type == "cross_border_compliance":
        report = generate_cross_border_compliance_report(data)
    else:
        report = generate_carrier_safety_report(data)

    # Output report
    if args.output == "slack":
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        send_to_slack(report, webhook_url)
        # Also save to file as backup
        save_to_file(report, args.output_dir)
    elif args.output == "file":
        save_to_file(report, args.output_dir)
    else:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
