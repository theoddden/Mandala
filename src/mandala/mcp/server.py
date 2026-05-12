"""Mandala MCP server — wraps the StateStore + connectors as LLM tools.

Uses the official ``mcp`` Python SDK (Anthropic). Run via ``mandala mcp``.
The server is stateless across calls; all data comes from Redis state.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import redis.asyncio as redis

from mandala.core.state import StateStore
from mandala.settings import get_settings


async def _state() -> tuple[StateStore, object]:
    r = redis.from_url(get_settings().redis_url, decode_responses=False)
    return StateStore(r), r


# --- Tool implementations ------------------------------------------------


async def tool_get_shipment(shipment_urn: str) -> dict[str, Any]:
    state, r = await _state()
    try:
        shipment = await state.get("shipment", shipment_urn)
        timeline = await state.read_timeline(shipment_urn)
        truck_urn = None
        if shipment is not None:
            v = await r.get(f"mandala:link:shipment:{shipment_urn}")
            truck_urn = v.decode() if isinstance(v, bytes) else v
        return {"shipment": shipment, "timeline": timeline, "truck_urn": truck_urn}
    finally:
        await r.aclose()


async def tool_get_truck(truck_urn: str) -> dict[str, Any]:
    state, r = await _state()
    try:
        truck = await state.get("truck", truck_urn)
        shipment_urn = await state.shipment_for_truck(truck_urn)
        return {"truck": truck, "shipment_urn": shipment_urn}
    finally:
        await r.aclose()


async def tool_check_customs_status(shipment_urn: str) -> dict[str, Any]:
    state, r = await _state()
    try:
        s = await state.get("shipment", shipment_urn) or {}
        return {
            "shipment_urn": shipment_urn,
            "customs_status": s.get("customs_status", "unknown"),
            "authority": s.get("authority"),
            "hold_reason": s.get("hold_reason"),
            "broker": s.get("broker_name") or (s.get("broker") or {}).get("name"),
            "filed_at": s.get("filed_at"),
            "released_at": s.get("released_at"),
        }
    finally:
        await r.aclose()


async def tool_get_recent_alerts(limit: int = 50, severity: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    r = redis.from_url(settings.redis_url, decode_responses=False)
    try:
        # Read the inbound stream from the tail; filter to alert types.
        resp = await r.xrevrange(settings.stream_inbound, count=max(limit * 5, 100))
        alerts: list[dict[str, Any]] = []
        for _msg_id, fields in resp or []:
            raw = fields.get(b"e") if isinstance(fields, dict) else None
            if raw is None:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not str(event.get("type", "")).startswith("mandala.alert."):
                continue
            if severity and (event.get("data") or {}).get("severity") != severity:
                continue
            alerts.append(event)
            if len(alerts) >= limit:
                break
        return {"alerts": alerts, "count": len(alerts)}
    finally:
        await r.aclose()


async def tool_get_fleet_near_border(border_poe: str, radius_km: float = 50.0) -> dict[str, Any]:
    """Return trucks near a Port-of-Entry.

    Prefers the geospatial materialized view (O(log N) ``GEOSEARCH``).
    Falls back to a state-store scan + Haversine if the view is empty, so
    this tool still works before ``mandala views`` has been run.
    """
    from mandala.core.state import _key
    from mandala.views.geospatial import GeospatialView

    state, r = await _state()
    target = BORDER_POE_COORDS.get(border_poe.lower())
    radius_mi = radius_km * 0.621371
    try:
        # --- Fast path: geospatial view --------------------------------
        if target is not None:
            view = GeospatialView(r)
            near = await view.trucks_near(lat=target[0], lon=target[1], radius_mi=radius_mi, limit=200)
            if near:
                return {
                    "border_poe": border_poe,
                    "radius_km": radius_km,
                    "source": "geospatial_view",
                    "trucks": [
                        {
                            "truck_urn": t["truck_urn"],
                            "distance_km": round(t["distance_mi"] * 1.609344, 2),
                            "last_seen_at": t.get("last_seen_at"),
                        }
                        for t in near
                    ],
                }

        # --- Fallback: state-store scan (pre-view deployments) ---------
        keys = []
        cursor = 0
        while True:
            cursor, batch = await r.scan(cursor, match=_key("truck", "*"), count=200)
            keys.extend(batch)
            if cursor == 0:
                break
        results: list[dict[str, Any]] = []
        for key in keys:
            urn = key.decode().split(":", 2)[-1] if isinstance(key, bytes) else key.split(":", 2)[-1]
            t = await state.get("truck", urn)
            if not t:
                continue
            pos = t.get("last_position") or {}
            lat, lon = pos.get("lat"), pos.get("lon")
            if lat is None or lon is None or target is None:
                continue
            d = _haversine_km(lat, lon, target[0], target[1])
            if d <= radius_km:
                results.append({"truck_urn": urn, "distance_km": round(d, 2), "last_seen_at": t.get("last_seen_at")})
        results.sort(key=lambda x: x["distance_km"])
        return {
            "border_poe": border_poe,
            "radius_km": radius_km,
            "source": "state_scan_fallback",
            "trucks": results,
        }
    finally:
        await r.aclose()


async def tool_get_trucks_at_poe_without_filing(poe: str) -> dict[str, Any]:
    """Return trucks physically present at a POE whose linked shipment has
    no released customs filing. Backed by the bitmap materialized view —
    O(bitmap-size / 8) via ``BITOP AND NOT``."""
    from mandala.views.bitmap import BitmapView

    state, r = await _state()
    try:
        view = BitmapView(r, state)
        urns = await view.at_poe_without_filing(poe)
        return {"poe": poe, "count": len(urns), "trucks": urns}
    finally:
        await r.aclose()


async def tool_get_cold_chain_breaches(since_hours: int = 24, limit: int = 100) -> dict[str, Any]:
    """Return cold-chain breach events across the fleet within the last
    ``since_hours`` hours. Backed by the timeseries materialized view."""
    from datetime import UTC, datetime, timedelta

    from mandala.views.timeseries import TimeseriesView

    state, r = await _state()
    try:
        view = TimeseriesView(r)
        since = (datetime.now(UTC) - timedelta(hours=since_hours)).timestamp()
        breaches = await view.recent_breaches(since_epoch=since, limit=limit)
        return {"since_hours": since_hours, "count": len(breaches), "breaches": breaches}
    finally:
        await r.aclose()


async def tool_get_entity_neighbors(urn: str, depth: int = 2) -> dict[str, Any]:
    """Multi-hop graph traversal from an entity URN. Requires the graph
    view (RedisGraph / FalkorDB module). Returns an empty neighbor list if
    the module is not available."""
    from mandala.views.graph import GraphView

    state, r = await _state()
    try:
        view = GraphView(r)
        neighbors = await view.neighbors(urn, depth=depth)
        return {"urn": urn, "depth": depth, "neighbors": neighbors}
    finally:
        await r.aclose()


async def tool_get_dead_zones_near(lat: float, lon: float, radius_km: float = 50.0, limit: int = 100) -> dict[str, Any]:
    """Return dead zones (connectivity gaps) within radius_km of a location."""
    from mandala.views.geospatial import GeospatialView

    state, r = await _state()
    try:
        view = GeospatialView(r)
        radius_mi = radius_km * 0.621371
        dead_zones = await view.dead_zones_near(lat=lat, lon=lon, radius_mi=radius_mi, limit=limit)
        return {
            "center_lat": lat,
            "center_lon": lon,
            "radius_km": radius_km,
            "count": len(dead_zones),
            "dead_zones": dead_zones,
        }
    finally:
        await r.aclose()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import asin, cos, radians, sin, sqrt

    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


# A small built-in coordinate map. Expand via a seed in dbt-mandala or via
# an env-loaded JSON file in production.
BORDER_POE_COORDS: dict[str, tuple[float, float]] = {
    "us-mx:laredo-tx": (27.5036, -99.5076),
    "us-mx:otay-mesa-ca": (32.5527, -116.9384),
    "us-mx:el-paso-tx": (31.7587, -106.4869),
    "us-mx:nogales-az": (31.3404, -110.9426),
    "us-mx:pharr-tx": (26.1948, -98.1836),
    "us-ca:detroit-mi": (42.3097, -83.0750),
    "us-ca:buffalo-ny": (42.9006, -78.9009),
    "us-ca:blaine-wa": (49.0028, -122.7569),
}


# --- MCP server wiring ---------------------------------------------------


def build_server():  # type: ignore[no-untyped-def]
    """Construct the MCP server with all Mandala tools registered."""
    try:
        from mcp.server import Server
        from mcp.types import TextContent, Tool
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("mcp package not installed. Install with: pip install mandala-bridge[mcp]") from exc

    server = Server("mandala")

    @server.list_tools()
    async def _list() -> list:
        return [
            Tool(
                name="get_shipment",
                description="Return the canonical Shipment object plus its timeline.",
                inputSchema={
                    "type": "object",
                    "properties": {"shipment_urn": {"type": "string"}},
                    "required": ["shipment_urn"],
                },
            ),
            Tool(
                name="get_truck",
                description="Return last-known truck position, telemetry, and linked shipment.",
                inputSchema={
                    "type": "object",
                    "properties": {"truck_urn": {"type": "string"}},
                    "required": ["truck_urn"],
                },
            ),
            Tool(
                name="check_customs_status",
                description="Return customs status, authority, broker, and any hold reason for a shipment.",
                inputSchema={
                    "type": "object",
                    "properties": {"shipment_urn": {"type": "string"}},
                    "required": ["shipment_urn"],
                },
            ),
            Tool(
                name="get_recent_alerts",
                description="Return the most recent Mandala alert events, optionally filtered by severity.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 50},
                        "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                    },
                },
            ),
            Tool(
                name="get_fleet_near_border",
                description="Return trucks within radius_km of a Port-of-Entry (e.g. 'us-mx:laredo-tx').",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "border_poe": {"type": "string"},
                        "radius_km": {"type": "number", "default": 50},
                    },
                    "required": ["border_poe"],
                },
            ),
            Tool(
                name="get_trucks_at_poe_without_filing",
                description=(
                    "Return trucks at a border Port-of-Entry that do not have a "
                    "released customs filing on their linked shipment. Backed by "
                    "the bitmap materialized view."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {"poe": {"type": "string"}},
                    "required": ["poe"],
                },
            ),
            Tool(
                name="get_cold_chain_breaches",
                description=(
                    "Return cold-chain breach events across the fleet within the "
                    "last N hours, ordered oldest → newest."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "since_hours": {"type": "integer", "default": 24},
                        "limit": {"type": "integer", "default": 100},
                    },
                },
            ),
            Tool(
                name="get_entity_neighbors",
                description=(
                    "Multi-hop graph traversal from a URN up to ``depth`` hops. "
                    "Requires the RedisGraph/FalkorDB module; returns empty if absent."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "urn": {"type": "string"},
                        "depth": {"type": "integer", "default": 2, "minimum": 1, "maximum": 5},
                    },
                    "required": ["urn"],
                },
            ),
            Tool(
                name="get_dead_zones_near",
                description=(
                    "Return dead zones (connectivity gaps) within radius_km of a location. "
                    "Dead zones are empirically derived from ping drop-offs."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "lat": {"type": "number"},
                        "lon": {"type": "number"},
                        "radius_km": {"type": "number", "default": 50.0},
                        "limit": {"type": "integer", "default": 100},
                    },
                    "required": ["lat", "lon"],
                },
            ),
        ]

    @server.call_tool()
    async def _call(name: str, arguments: dict[str, Any]) -> list:
        if name == "get_shipment":
            result = await tool_get_shipment(arguments["shipment_urn"])
        elif name == "get_truck":
            result = await tool_get_truck(arguments["truck_urn"])
        elif name == "check_customs_status":
            result = await tool_check_customs_status(arguments["shipment_urn"])
        elif name == "get_recent_alerts":
            result = await tool_get_recent_alerts(
                limit=int(arguments.get("limit", 50)),
                severity=arguments.get("severity"),
            )
        elif name == "get_fleet_near_border":
            result = await tool_get_fleet_near_border(
                border_poe=arguments["border_poe"],
                radius_km=float(arguments.get("radius_km", 50)),
            )
        elif name == "get_trucks_at_poe_without_filing":
            result = await tool_get_trucks_at_poe_without_filing(poe=arguments["poe"])
        elif name == "get_cold_chain_breaches":
            result = await tool_get_cold_chain_breaches(
                since_hours=int(arguments.get("since_hours", 24)),
                limit=int(arguments.get("limit", 100)),
            )
        elif name == "get_entity_neighbors":
            result = await tool_get_entity_neighbors(
                urn=arguments["urn"],
                depth=int(arguments.get("depth", 2)),
            )
        elif name == "get_dead_zones_near":
            result = await tool_get_dead_zones_near(
                lat=float(arguments["lat"]),
                lon=float(arguments["lon"]),
                radius_km=float(arguments.get("radius_km", 50.0)),
                limit=int(arguments.get("limit", 100)),
            )
        else:
            raise ValueError(f"unknown tool: {name}")
        return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

    return server


def main() -> None:
    """Entry point used by ``mandala mcp``."""
    try:
        from mcp.server.stdio import stdio_server
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("mcp package not installed. Install with: pip install mandala-bridge[mcp]") from exc

    async def _run() -> None:
        server = build_server()
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    asyncio.run(_run())
