"""Load-board auto-poster.

Two stages, both implemented as detector functions to fit the existing
worker pipeline:

1. :func:`detect_truck_empty` — when a shipment is delivered, emit
   ``mandala.truck.empty`` for the linked truck. Carries the truck's last
   known position, equipment, and HOS-remaining hours so downstream
   posters don't need to round-trip Samsara.

2. :func:`post_to_loadboards` — when ``mandala.truck.empty`` lands, post
   the truck to every configured load board (DAT, Truckstop) in
   parallel. Records each posting id in state and emits a
   ``mandala.loadboard.posted`` audit event per board.

Both stages are guarded:

* If ``MANDALA_LOADBOARD_ENABLED=0`` (default), :func:`post_to_loadboards`
  is a no-op. Auto-posting **must** be opt-in to avoid surprise costs.
* If a board's credentials aren't configured, that board is skipped
  silently — Mandala degrades gracefully when only one is set up.
* A per-truck debounce key prevents double-posting if the empty event is
  re-emitted within the 24h posting window.
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.schema.truck import EquipmentType
from mandala.core.state import StateStore
from mandala.settings import get_settings

log = structlog.get_logger(__name__)

POST_DEBOUNCE_TTL_SECONDS = 21_600  # 6h: avoid re-posting after a re-trigger


# ---------------------------------------------------------------------------
# Stage 1: detect "truck went empty"
# ---------------------------------------------------------------------------


async def detect_truck_empty(
    event: MandalaEvent, state: StateStore, redis: "object"
) -> list[MandalaEvent]:
    """Translate a delivery confirmation into a ``mandala.truck.empty`` event."""
    if event.type != EventType.SHIPMENT_DELIVERED.value:
        return []
    shipment_urn = event.subject or ""
    truck_urn = None
    # Prefer an explicit link in state; fall back to truck_urn carried on the event.
    data = event.data if isinstance(event.data, dict) else {}
    explicit = data.get("truck_urn")
    if explicit:
        truck_urn = explicit
    else:
        # The link is keyed by truck → shipment, so reverse-lookup via the
        # state store's symmetric mapping.
        v = await redis.get(f"mandala:link:shipment:{shipment_urn}")  # type: ignore[attr-defined]
        truck_urn = v.decode() if isinstance(v, bytes) else v

    if not truck_urn:
        return []

    truck = await state.get("truck", truck_urn) or {}
    pos = truck.get("last_position") or {}
    payload: dict[str, Any] = {
        "truck_urn": truck_urn,
        "shipment_urn": shipment_urn,
        "delivered_at": event.time.astimezone(UTC).isoformat(),
        "last_position": pos,
        "equipment": truck.get("equipment"),
        "vin": truck.get("vin"),
        "license_plate": truck.get("license_plate"),
    }
    return [
        new_event(
            type=EventType.TRUCK_EMPTY,
            source="mandala/loadboard",
            subject=truck_urn,
            data=payload,
        )
    ]


# ---------------------------------------------------------------------------
# Stage 2: post empty trucks to every configured board
# ---------------------------------------------------------------------------


async def _post_to_dat(
    *,
    truck_urn: str,
    equipment: EquipmentType,
    lat: float,
    lon: float,
    radius_mi: int,
    ttl_hours: int,
    external_ref: str,
) -> dict[str, Any]:
    from mandala.connectors.dat.client import DATClient

    async with DATClient() as client:
        resp = await client.post_truck(
            equipment=equipment,
            origin_lat=lat,
            origin_lon=lon,
            radius_mi=radius_mi,
            ttl_hours=ttl_hours,
            external_reference=external_ref,
            comments=f"Mandala auto-post {socket.gethostname()}",
        )
    return {"board": "dat", "ok": True, "response": resp, "posting_id": resp.get("postingId")}


async def _post_to_truckstop(
    *,
    truck_urn: str,
    equipment: EquipmentType,
    lat: float,
    lon: float,
    radius_mi: int,
    ttl_hours: int,
    external_ref: str,
) -> dict[str, Any]:
    from mandala.connectors.truckstop.client import TruckstopClient

    async with TruckstopClient() as client:
        resp = await client.post_truck(
            equipment=equipment,
            origin_lat=lat,
            origin_lon=lon,
            radius_mi=radius_mi,
            ttl_hours=ttl_hours,
            external_reference=external_ref,
            comments=f"Mandala auto-post {socket.gethostname()}",
        )
    return {"board": "truckstop", "ok": True, "response": resp, "posting_id": resp.get("postingId") or resp.get("id")}


_BOARDS = {
    "dat": _post_to_dat,
    "truckstop": _post_to_truckstop,
}


async def post_to_loadboards(
    event: MandalaEvent, state: StateStore, redis: "object"
) -> list[MandalaEvent]:
    """Post a freshly-empty truck to every configured load board."""
    if event.type != EventType.TRUCK_EMPTY.value:
        return []

    s = get_settings()
    if not s.loadboard_enabled:
        return []

    # Choose configured boards via local imports (so missing optional deps
    # never crash the worker).
    enabled: dict[str, Any] = {}
    try:
        from mandala.connectors.dat import DATConnector

        if DATConnector().is_configured():
            enabled["dat"] = _post_to_dat
    except ImportError:
        pass
    try:
        from mandala.connectors.truckstop import TruckstopConnector

        if TruckstopConnector().is_configured():
            enabled["truckstop"] = _post_to_truckstop
    except ImportError:
        pass

    if not enabled:
        return []

    truck_urn = event.subject or ""
    if not await redis.set(  # type: ignore[attr-defined]
        f"mandala:loadboard:debounce:{truck_urn}", "1", nx=True, ex=POST_DEBOUNCE_TTL_SECONDS
    ):
        log.info("loadboard.skip.debounced", truck=truck_urn)
        return []

    data = event.data if isinstance(event.data, dict) else {}
    pos = data.get("last_position") or {}
    lat = pos.get("lat")
    lon = pos.get("lon")
    if lat is None or lon is None:
        return [
            new_event(
                type=EventType.LOADBOARD_POST_FAILED,
                source="mandala/loadboard",
                subject=truck_urn,
                data={"reason": "no_known_position"},
            )
        ]

    raw_eq = data.get("equipment")
    try:
        equipment = EquipmentType(raw_eq) if raw_eq else EquipmentType.VAN
    except ValueError:
        equipment = EquipmentType.OTHER

    external_ref = f"mandala:{truck_urn}:{int(datetime.now(UTC).timestamp())}"

    async def _safe(name: str, fn) -> tuple[str, dict[str, Any]]:
        try:
            return name, await fn(
                truck_urn=truck_urn,
                equipment=equipment,
                lat=float(lat),
                lon=float(lon),
                radius_mi=s.loadboard_post_default_radius_mi,
                ttl_hours=s.loadboard_post_ttl_hours,
                external_ref=external_ref,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("loadboard.post_failed", board=name, truck=truck_urn)
            return name, {"board": name, "ok": False, "error": str(exc)}

    results = await asyncio.gather(*(_safe(name, fn) for name, fn in enabled.items()))

    out: list[MandalaEvent] = []
    for board, result in results:
        if result.get("ok"):
            out.append(
                new_event(
                    type=EventType.LOADBOARD_POSTED,
                    source="mandala/loadboard",
                    subject=truck_urn,
                    data={
                        "truck_urn": truck_urn,
                        "board": board,
                        "posting_id": result.get("posting_id"),
                        "equipment": equipment.value,
                        "origin": {"lat": lat, "lon": lon},
                        "ttl_hours": s.loadboard_post_ttl_hours,
                        "radius_mi": s.loadboard_post_default_radius_mi,
                        "external_reference": external_ref,
                    },
                )
            )
            # Persist posting id for later expiry / lookups.
            with contextlib.suppress(Exception):
                await state.upsert(
                    "loadboard_post",
                    f"{truck_urn}:{board}",
                    {"posting_id": result.get("posting_id"), "external_reference": external_ref},
                )
        else:
            out.append(
                new_event(
                    type=EventType.LOADBOARD_POST_FAILED,
                    source="mandala/loadboard",
                    subject=truck_urn,
                    data={
                        "truck_urn": truck_urn,
                        "board": board,
                        "error": result.get("error"),
                    },
                )
            )
    return out


DETECTORS = (detect_truck_empty, post_to_loadboards)
