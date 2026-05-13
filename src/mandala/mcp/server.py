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
    from mandala.views.dead_zone import DeadZoneView

    state, r = await _state()
    try:
        view = DeadZoneView(r)
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


async def tool_replay_events(
    from_dt: str,
    to_dt: str,
    entity_urn: str | None = None,
    dry_run: bool = False,
    count: int = 1000,
) -> dict[str, Any]:
    """Replay historical events to fix state after bugs. Can replay from Redis Stream (recent events)."""
    from datetime import datetime

    from mandala.core.replay import replay_from_stream

    settings = get_settings()
    r = redis.from_url(settings.redis_url, decode_responses=False)
    state = StateStore(r)

    try:
        # Replay from Redis Stream (recent events)
        stats = await replay_from_stream(r, state, settings.stream_inbound, count, dry_run)
        return stats
    finally:
        await r.aclose()


async def tool_query_event_log(
    subject: str | None = None,
    event_type: str | None = None,
    from_dt: str | None = None,
    to_dt: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query the Iceberg event log with filters (if enabled)."""
    from datetime import datetime

    from mandala.core.event_log import get_event_log

    event_log = get_event_log()
    if not event_log:
        return {"error": "Event log not configured. Set MANDALA_EVENT_LOG_ENABLED=1"}

    try:
        time_range = None
        if from_dt and to_dt:
            time_range = (datetime.fromisoformat(from_dt), datetime.fromisoformat(to_dt))

        events = []
        async for event in event_log.query(subject=subject, event_type=event_type, time_range=time_range):
            events.append(event.model_dump_json(exclude_none=True, by_alias=True))
            if len(events) >= limit:
                break

        return {"events": events, "count": len(events)}
    except Exception as exc:
        return {"error": str(exc), "events": [], "count": 0}


async def tool_inspect_dlq(limit: int = 50) -> dict[str, Any]:
    """Inspect the Dead Letter Queue for failed events."""
    from mandala.core.dead_letter import DeadLetterQueue

    settings = get_settings()
    r = redis.from_url(settings.redis_url, decode_responses=False)
    try:
        dlq = DeadLetterQueue(r)
        failed_events = await dlq.read(count=limit)
        stats = await dlq.stats()
        return {"dlq_stats": stats, "events": failed_events}
    finally:
        await r.aclose()


async def tool_health_check() -> dict[str, Any]:
    """Return health status of Mandala components (Redis, stream, event log)."""
    from mandala.core.event_log import get_event_log

    settings = get_settings()
    r = redis.from_url(settings.redis_url, decode_responses=False)
    try:
        # Check Redis connectivity
        await r.ping()
        health_status: dict[str, Any] = {"status": "healthy", "checks": {"redis": "ok"}}

        # Check stream health
        try:
            info = await r.xinfo_stream(settings.stream_inbound)
            health_status["checks"]["stream"] = "ok"
            health_status["checks"]["stream_length"] = info.get("length", 0)
            health_status["checks"]["stream_groups"] = info.get("groups", 0)
        except Exception:
            health_status["checks"]["stream"] = "failed"
            health_status["status"] = "degraded"

        # Check event log
        event_log = get_event_log()
        if event_log:
            health_status["checks"]["event_log"] = "enabled"
        else:
            health_status["checks"]["event_log"] = "disabled"

        return health_status
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc), "checks": {"redis": "failed"}}
    finally:
        await r.aclose()


async def tool_get_connector_status() -> dict[str, Any]:
    """Return configuration status of all Mandala connectors."""
    settings = get_settings()
    return {
        "samsara": {
            "webhook_configured": bool(settings.samsara_webhook_secret),
            "outbound_enabled": settings.samsara_outbound_enabled,
            "api_token_configured": bool(settings.samsara_api_token),
        },
        "descartes": {
            "webhook_configured": bool(settings.descartes_webhook_secret),
            "api_key_configured": bool(settings.descartes_api_key),
        },
        "cargowise": {
            "webhook_configured": bool(settings.cargowise_webhook_secret),
            "eadaptor_configured": bool(settings.cargowise_eadaptor_url),
        },
        "vizion": {"api_key_configured": bool(settings.vizion_api_key)},
        "loadboard": {
            "enabled": settings.loadboard_enabled,
            "dat_configured": bool(settings.dat_client_id and settings.dat_client_secret),
        },
        "fmcsa": {"enabled": True},  # FMCSA uses public API, always enabled
        "fuel_cards": {
            "coast_configured": bool(settings.coast_api_key),
            "fleetcor_configured": bool(settings.fleetcor_api_key),
            "wex_configured": bool(settings.wex_api_key),
            "efs_configured": bool(settings.efs_api_key),
        },
    }


async def tool_get_bridge_capabilities() -> dict[str, Any]:
    """Return self-describing bridge metadata: active connectors, event schemas, and mappings."""
    from mandala.core.events.types import EventType

    settings = get_settings()

    # Connector-specific event type mappings (from connector normalize() functions)
    connector_event_types = {
        "samsara": {
            "supported_event_types": [
                "mandala.truck.position",
                "mandala.truck.geofence.entered",
                "mandala.truck.geofence.exited",
                "mandala.truck.poe.entered",
                "mandala.truck.poe.exited",
                "mandala.cold_chain.reading",
                "mandala.cold_chain.breach",
                "mandala.driver.hos.warning",
                "mandala.driver.log.violation",
                "mandala.truck.harsh_event",
            ],
            "vendor_payload_mapping": {
                "VehicleLocation": "mandala.truck.position",
                "VehicleGpsUpdated": "mandala.truck.position",
                "VehicleGpsUpdate": "mandala.truck.position",
                "VehicleEnterGeofence": "mandala.truck.geofence.entered",
                "GeofenceEntry": "mandala.truck.geofence.entered",
                "VehicleExitGeofence": "mandala.truck.geofence.exited",
                "GeofenceExit": "mandala.truck.geofence.exited",
                "VehicleTemperature": "mandala.cold_chain.reading",
                "TemperatureSensorReading": "mandala.cold_chain.reading",
                "TemperatureExceeded": "mandala.cold_chain.breach",
                "EldHosViolation": "mandala.driver.log.violation",
                "HosViolationDetected": "mandala.driver.log.violation",
                "HarshEvent": "mandala.truck.harsh_event",
                "VehicleHarshEvent": "mandala.truck.harsh_event",
            },
        },
        "descartes": {
            "supported_event_types": [
                "mandala.shipment.booked",
                "mandala.shipment.dispatched",
                "mandala.shipment.picked_up",
                "mandala.shipment.in_transit",
                "mandala.shipment.at_border",
                "mandala.shipment.delivered",
                "mandala.shipment.cancelled",
                "mandala.shipment.customs.hold.landed",
                "mandala.shipment.customs.hold.cleared",
                "mandala.shipment.customs.documentation.missing",
                "mandala.shipment.customs.inspection.required",
                "mandala.shipment.eta.updated",
            ],
            "vendor_payload_mapping": {
                "TrackingRequest": "mandala.shipment.booked",
                "StatusUpdate": "mandala.shipment.in_transit",
                "LocationUpdate": "mandala.shipment.in_transit",
            },
        },
    }

    # Build connector status with event type info
    connectors = {}
    for connector_name, event_info in connector_event_types.items():
        if connector_name == "samsara":
            connectors[connector_name] = {
                "active": bool(settings.samsara_webhook_secret),
                "webhook_configured": bool(settings.samsara_webhook_secret),
                "outbound_enabled": settings.samsara_outbound_enabled,
                "api_token_configured": bool(settings.samsara_api_token),
                **event_info,
            }
        elif connector_name == "descartes":
            connectors[connector_name] = {
                "active": bool(settings.descartes_webhook_secret),
                "webhook_configured": bool(settings.descartes_webhook_secret),
                "api_key_configured": bool(settings.descartes_api_key),
                **event_info,
            }

    # Canonical event registry (from EventType enum)
    canonical_event_registry = {}
    for event_type in EventType:
        canonical_event_registry[event_type.value] = {
            "description": event_type.name.replace("_", " ").lower(),
        }

    # POE geofence configuration
    poe_geofences = {
        "configured": bool(settings.samsara_webhook_secret),  # Only Samsara supports POE geofences currently
        "active_geofences": list(BORDER_POE_COORDS.keys()),
    }

    # MCP tools available
    mcp_tools = {
        "core_queries": [
            {"name": "get_shipment", "description": "Return canonical Shipment object plus timeline"},
            {"name": "get_truck", "description": "Return last-known truck position, telemetry, and linked shipment"},
            {"name": "check_customs_status", "description": "Return customs status, authority, broker, and hold reason"},
        ],
        "alerts_monitoring": [
            {"name": "get_recent_alerts", "description": "Return recent Mandala alert events, optionally filtered by severity"},
            {"name": "get_cold_chain_breaches", "description": "Return cold-chain breach events across the fleet"},
        ],
        "geospatial_queries": [
            {"name": "get_fleet_near_border", "description": "Return trucks within radius_km of a Port-of-Entry"},
            {"name": "get_truck_position", "description": "Return current position of a truck from geospatial view"},
            {"name": "get_dead_zones_near", "description": "Return dead zones (connectivity gaps) near a location"},
        ],
        "border_operations": [
            {"name": "get_trucks_at_poe_without_filing", "description": "Return trucks at POE without released customs filing"},
            {"name": "get_trucks_at_poe", "description": "Return all trucks currently at a Port-of-Entry"},
        ],
        "materialized_views": [
            {"name": "get_entity_neighbors", "description": "Multi-hop graph traversal from a URN"},
            {"name": "get_temperature_readings", "description": "Return temperature readings for a specific truck over time"},
            {"name": "materialized_views_health", "description": "Return health status of all materialized views"},
        ],
        "external_integrations": [
            {"name": "get_rail_status", "description": "Fetch rail intermodal status from Vizion API"},
            {"name": "get_fmcsa_carrier_info", "description": "Fetch carrier safety data from FMCSA SAFER database"},
        ],
        "operations": [
            {"name": "replay_events", "description": "Replay historical events to fix state after bugs"},
            {"name": "query_event_log", "description": "Query the Iceberg event log with filters"},
            {"name": "inspect_dlq", "description": "Inspect the Dead Letter Queue for failed events"},
        ],
        "health_connectors": [
            {"name": "health_check", "description": "Return health status of Mandala components"},
            {"name": "get_connector_status", "description": "Return configuration status of all Mandala connectors"},
            {"name": "get_bridge_capabilities", "description": "Return self-describing bridge metadata"},
        ],
        "compliance_zk": [
            {"name": "query_zk_proofs", "description": "Query ZK proof status for cold-chain breaches"},
            {"name": "validate_schema", "description": "Validate a Mandala vendor schema file"},
        ],
    }

    return {
        "bridge_version": "0.3.1",
        "schema_version": "0.3",
        "mcp_tools": mcp_tools,
        "connectors": connectors,
        "canonical_event_registry": canonical_event_registry,
        "poe_geofences": poe_geofences,
    }


async def tool_query_zk_proofs(proof_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    """Query ZK proof status for cold-chain breaches (if ZK is enabled)."""
    settings = get_settings()
    if not settings.zk_enabled:
        return {"error": "ZK proving not enabled. Set MANDALA_ZK_ENABLED=1"}

    # ZK proofs are stored as events in the event log
    from mandala.core.event_log import get_event_log

    event_log = get_event_log()
    if not event_log:
        return {"error": "Event log not configured. ZK proofs require event log."}

    try:
        events = []
        async for event in event_log.query(event_type="mandala.zk.proof.generated"):
            events.append(event.model_dump_json(exclude_none=True, by_alias=True))
            if len(events) >= limit:
                break

        if proof_id:
            events = [e for e in events if e.get("data", {}).get("proof_id") == proof_id]

        return {"proofs": events, "count": len(events)}
    except Exception as exc:
        return {"error": str(exc), "proofs": [], "count": 0}


async def tool_validate_schema(schema_path: str) -> dict[str, Any]:
    """Validate a Mandala vendor schema file."""
    from pathlib import Path

    schema_file = Path(schema_path)
    if not schema_file.exists():
        return {"error": f"Schema file not found: {schema_path}"}

    try:
        import asyncio

        import yaml

        # Use asyncio.to_thread to avoid blocking the event loop
        def _load_schema():
            with open(schema_file) as f:
                return yaml.safe_load(f)

        schema = await asyncio.to_thread(_load_schema)

        # Check required fields
        required_fields = [
            "vendor",
            "canonical_type",
            "description",
            "mapping",
            "example_vendor_payload",
            "required_fields",
        ]
        missing = [f for f in required_fields if f not in schema]
        if missing:
            return {"valid": False, "error": f"Missing required fields: {missing}"}

        # Validate mapping is a dict
        if not isinstance(schema.get("mapping"), dict):
            return {"valid": False, "error": "Mapping must be a dictionary"}

        # Validate required_fields is a list
        if not isinstance(schema.get("required_fields"), list):
            return {"valid": False, "error": "required_fields must be a list"}

        return {
            "valid": True,
            "vendor": schema.get("vendor"),
            "canonical_type": schema.get("canonical_type"),
            "mapping_fields": len(schema.get("mapping", {})),
        }
    except ImportError:
        return {"valid": False, "error": "PyYAML not installed. Install with: pip install pyyaml"}
    except Exception as exc:
        return {"valid": False, "error": str(exc)}


async def tool_get_truck_position(truck_urn: str) -> dict[str, Any]:
    """Return the current position of a truck from the geospatial view."""
    from mandala.views.geospatial import GeospatialView

    state, r = await _state()
    try:
        view = GeospatialView(r)
        position = await view.truck_position(truck_urn)
        if position is None:
            return {"truck_urn": truck_urn, "error": "Truck not found in geospatial view"}
        return position
    finally:
        await r.aclose()


async def tool_get_trucks_at_poe(poe: str) -> dict[str, Any]:
    """Return all trucks currently at a Port-of-Entry (regardless of filing status)."""
    from mandala.views.bitmap import BitmapView

    state, r = await _state()
    try:
        view = BitmapView(r, state)
        urns = await view.at_poe(poe)
        return {"poe": poe, "count": len(urns), "trucks": urns}
    finally:
        await r.aclose()


async def tool_get_temperature_readings(
    truck_urn: str,
    since_hours: int = 24,
    until_hours: int = 0,
    limit: int = 1000,
) -> dict[str, Any]:
    """Return temperature readings for a specific truck within a time range."""
    from datetime import UTC, datetime, timedelta

    from mandala.views.timeseries import TimeseriesView

    state, r = await _state()
    try:
        view = TimeseriesView(r)
        since = (datetime.now(UTC) - timedelta(hours=since_hours)).timestamp()
        until = (datetime.now(UTC) - timedelta(hours=until_hours)).timestamp()
        readings = await view.range(truck_urn, since, until, limit)
        return {
            "truck_urn": truck_urn,
            "since_hours": since_hours,
            "until_hours": until_hours,
            "count": len(readings),
            "readings": readings,
        }
    finally:
        await r.aclose()


async def tool_materialized_views_health() -> dict[str, Any]:
    """Return health status of all materialized views."""
    from mandala.views.bitmap import BitmapView
    from mandala.views.dead_zone import DeadZoneView
    from mandala.views.geospatial import GeospatialView
    from mandala.views.graph import GraphView
    from mandala.views.timeseries import TimeseriesView

    state, r = await _state()
    try:
        views = {
            "geospatial": GeospatialView(r),
            "timeseries": TimeseriesView(r),
            "bitmap": BitmapView(r, state),
            "graph": GraphView(r),
            "dead_zones": DeadZoneView(r),
        }
        health_status: dict[str, Any] = {"views": {}}
        for name, view in views.items():
            try:
                health_status["views"][name] = await view.health()
            except Exception as exc:
                health_status["views"][name] = {"name": name, "ok": False, "error": str(exc)}
        health_status["overall_status"] = all(v.get("ok", False) for v in health_status["views"].values())
        return health_status
    finally:
        await r.aclose()


async def tool_get_rail_status(container_id: str) -> dict[str, Any]:
    """Fetch rail intermodal status for a container from Vizion API."""
    from mandala.connectors.rail.vizion import VizionRailProvider

    try:
        provider = VizionRailProvider()
        if not provider.is_configured():
            return {"error": "Vizion rail provider not configured"}

        intermodal = provider.get_intermodal_status(str(container_id))
        return {
            "container_id": intermodal.container_id,
            "carrier_scac": intermodal.carrier_scac,
            "origin_ramp": intermodal.origin_ramp,
            "destination_ramp": intermodal.destination_ramp,
            "last_free_day": intermodal.last_free_day.isoformat() if intermodal.last_free_day else None,
            "available_for_pickup": intermodal.available_for_pickup,
            "eta": intermodal.eta.isoformat() if intermodal.eta else None,
            "provider": intermodal.provider,
            "retrieved_at": intermodal.retrieved_at.isoformat(),
            "milestones": [
                {
                    "event_type": m.event_type,
                    "location": m.location,
                    "timestamp": m.timestamp.isoformat(),
                    "timezone": m.timezone,
                }
                for m in intermodal.milestones
            ],
        }
    except Exception as exc:
        return {"error": str(exc), "container_id": container_id}


async def tool_get_fmcsa_carrier_info(dot_number: str) -> dict[str, Any]:
    """Fetch carrier safety data from FMCSA SAFER database."""
    from mandala.connectors.fmcsa.client import FMCSAClient

    try:
        client = FMCSAClient()
        carrier = await client.get_carrier_snapshot(str(dot_number))
        if not carrier:
            return {"error": f"Carrier not found: {dot_number}"}
        return carrier.model_dump() if hasattr(carrier, "model_dump") else carrier
    except Exception as exc:
        return {"error": str(exc), "dot_number": dot_number}


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
            Tool(
                name="replay_events",
                description="Replay historical events from Redis Stream to fix state after bugs.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "from_dt": {"type": "string", "description": "Start datetime (ISO format)"},
                        "to_dt": {"type": "string", "description": "End datetime (ISO format)"},
                        "entity_urn": {"type": "string", "description": "Replay for specific entity URN only"},
                        "dry_run": {"type": "boolean", "default": False, "description": "Don't write to state"},
                        "count": {"type": "integer", "default": 1000, "description": "Number of events to replay"},
                    },
                    "required": ["from_dt", "to_dt"],
                },
            ),
            Tool(
                name="query_event_log",
                description="Query the Iceberg event log with filters (if enabled).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "subject": {"type": "string", "description": "Filter by subject URN"},
                        "event_type": {"type": "string", "description": "Filter by event type"},
                        "from_dt": {"type": "string", "description": "Start datetime (ISO format)"},
                        "to_dt": {"type": "string", "description": "End datetime (ISO format)"},
                        "limit": {"type": "integer", "default": 100},
                    },
                },
            ),
            Tool(
                name="inspect_dlq",
                description="Inspect the Dead Letter Queue for failed events.",
                inputSchema={
                    "type": "object",
                    "properties": {"limit": {"type": "integer", "default": 50}},
                },
            ),
            Tool(
                name="health_check",
                description="Return health status of Mandala components (Redis, stream, event log).",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_connector_status",
                description="Return configuration status of all Mandala connectors.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_bridge_capabilities",
                description="Return self-describing bridge metadata: MCP tools, active connectors, event schemas, vendor payload mappings, and POE geofence configuration.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="query_zk_proofs",
                description="Query ZK proof status for cold-chain breaches (if ZK is enabled).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "proof_id": {"type": "string", "description": "Filter by specific proof ID"},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            Tool(
                name="validate_schema",
                description="Validate a Mandala vendor schema file.",
                inputSchema={
                    "type": "object",
                    "properties": {"schema_path": {"type": "string"}},
                    "required": ["schema_path"],
                },
            ),
            Tool(
                name="get_truck_position",
                description="Return the current position of a truck from the geospatial view.",
                inputSchema={
                    "type": "object",
                    "properties": {"truck_urn": {"type": "string"}},
                    "required": ["truck_urn"],
                },
            ),
            Tool(
                name="get_trucks_at_poe",
                description="Return all trucks currently at a Port-of-Entry (regardless of filing status).",
                inputSchema={
                    "type": "object",
                    "properties": {"poe": {"type": "string"}},
                    "required": ["poe"],
                },
            ),
            Tool(
                name="get_temperature_readings",
                description="Return temperature readings for a specific truck within a time range.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "truck_urn": {"type": "string"},
                        "since_hours": {"type": "integer", "default": 24},
                        "until_hours": {"type": "integer", "default": 0},
                        "limit": {"type": "integer", "default": 1000},
                    },
                    "required": ["truck_urn"],
                },
            ),
            Tool(
                name="materialized_views_health",
                description="Return health status of all materialized views (geospatial, timeseries, bitmap, graph, dead_zones).",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_rail_status",
                description="Fetch rail intermodal status for a container from Vizion API.",
                inputSchema={
                    "type": "object",
                    "properties": {"container_id": {"type": "string"}},
                    "required": ["container_id"],
                },
            ),
            Tool(
                name="get_fmcsa_carrier_info",
                description="Fetch carrier safety data from FMCSA SAFER database by DOT number.",
                inputSchema={
                    "type": "object",
                    "properties": {"dot_number": {"type": "string"}},
                    "required": ["dot_number"],
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
        elif name == "replay_events":
            result = await tool_replay_events(
                from_dt=arguments["from_dt"],
                to_dt=arguments["to_dt"],
                entity_urn=arguments.get("entity_urn"),
                dry_run=bool(arguments.get("dry_run", False)),
                count=int(arguments.get("count", 1000)),
            )
        elif name == "query_event_log":
            result = await tool_query_event_log(
                subject=arguments.get("subject"),
                event_type=arguments.get("event_type"),
                from_dt=arguments.get("from_dt"),
                to_dt=arguments.get("to_dt"),
                limit=int(arguments.get("limit", 100)),
            )
        elif name == "inspect_dlq":
            result = await tool_inspect_dlq(limit=int(arguments.get("limit", 50)))
        elif name == "health_check":
            result = await tool_health_check()
        elif name == "get_connector_status":
            result = await tool_get_connector_status()
        elif name == "get_bridge_capabilities":
            result = await tool_get_bridge_capabilities()
        elif name == "query_zk_proofs":
            result = await tool_query_zk_proofs(
                proof_id=arguments.get("proof_id"),
                limit=int(arguments.get("limit", 50)),
            )
        elif name == "validate_schema":
            result = await tool_validate_schema(schema_path=arguments["schema_path"])
        elif name == "get_truck_position":
            result = await tool_get_truck_position(truck_urn=arguments["truck_urn"])
        elif name == "get_trucks_at_poe":
            result = await tool_get_trucks_at_poe(poe=arguments["poe"])
        elif name == "get_temperature_readings":
            result = await tool_get_temperature_readings(
                truck_urn=arguments["truck_urn"],
                since_hours=int(arguments.get("since_hours", 24)),
                until_hours=int(arguments.get("until_hours", 0)),
                limit=int(arguments.get("limit", 1000)),
            )
        elif name == "materialized_views_health":
            result = await tool_materialized_views_health()
        elif name == "get_rail_status":
            result = await tool_get_rail_status(container_id=arguments["container_id"])
        elif name == "get_fmcsa_carrier_info":
            result = await tool_get_fmcsa_carrier_info(dot_number=arguments["dot_number"])
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
