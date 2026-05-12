"""Mandala MCP server.

Exposes the canonical state and connectors as Model Context Protocol tools
so any LLM can ask logistics questions in natural language. Install with:

    pip install mandala-bridge[mcp]
    mandala mcp

Tools (v0.1):

* ``get_shipment(shipment_urn)`` — full canonical Shipment + timeline.
* ``get_truck(truck_urn)`` — last known position + telemetry.
* ``get_fleet_near_border(border_poe, radius_km=50)`` — trucks currently
  near a Port-of-Entry.
* ``check_customs_status(shipment_urn)`` — customs status, hold reason,
  authority, broker.
* ``get_recent_alerts(limit=50, severity=None)`` — most recent alerts.
"""

from mandala.mcp.server import build_server, main

__all__ = ["build_server", "main"]
