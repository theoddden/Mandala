"""Geospatial materialized view.

Maintains a Redis ``GEO`` set keyed by truck URN so that "trucks near X"
queries run in O(log N + M) instead of the current O(N) state-store scan
(``@mandala.mcp.server.tool_get_fleet_near_border``).

Query entry points:

* :meth:`GeospatialView.trucks_near` — ``GEOSEARCH`` around a lat/lon.
* :meth:`GeospatialView.truck_position` — ``GEOPOS`` for a single URN.
* :meth:`GeospatialView.last_seen` — hash lookup for staleness filtering.
"""

from __future__ import annotations

from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType
from mandala.views.base import MaterializedView

log = structlog.get_logger(__name__)

GEO_KEY = "mandala:view:geo:trucks"
SEEN_KEY = "mandala:view:geo:trucks:last_seen"
DEAD_ZONE_KEY = "mandala:view:geo:dead_zones"


class GeospatialView(MaterializedView):
    name = "geospatial"

    def __init__(self, redis: object) -> None:
        self._r = redis

    async def apply(self, event: MandalaEvent) -> None:
        # Handle truck position events for geospatial indexing
        if event.type == EventType.TRUCK_POSITION.value:
            data = event.data if isinstance(event.data, dict) else {}
            urn = event.subject or ""
            if not urn.startswith("urn:mandala:truck:"):
                return

            # Accept both the canonical TruckTelemetry shape and flat dicts.
            position = data.get("position") or {}
            point = position.get("point") or {}
            lat = point.get("lat") if isinstance(point, dict) else None
            lon = point.get("lon") if isinstance(point, dict) else None
            # Fallback: some normalizers put lat/lon directly on data.
            if lat is None:
                lat = data.get("lat") or (data.get("last_position") or {}).get("lat")
            if lon is None:
                lon = data.get("lon") or (data.get("last_position") or {}).get("lon")

            if lat is None or lon is None:
                return

            try:
                lon_f, lat_f = float(lon), float(lat)
            except (TypeError, ValueError):
                return

            # GEOADD is idempotent — repeated apply on the same URN just
            # overwrites the position.
            await self._r.geoadd(GEO_KEY, (lon_f, lat_f, urn))  # type: ignore[attr-defined]
            ts = event.time.isoformat() if event.time else ""
            if ts:
                await self._r.hset(SEEN_KEY, urn, ts)  # type: ignore[attr-defined]

        # Handle dead zone events for connectivity mapping
        elif event.type == "mandala.connectivity.dead_zone":
            data = event.data if isinstance(event.data, dict) else {}
            truck_urn = event.subject or ""
            lat = data.get("last_known_lat")
            lon = data.get("last_known_lon")
            gap_seconds = data.get("gap_duration_seconds")

            if lat is None or lon is None:
                return

            try:
                lon_f, lat_f = float(lon), float(lat)
            except (TypeError, ValueError):
                return

            # Store dead zone location with metadata
            member = f"{truck_urn}:{gap_seconds or 0}"
            await self._r.geoadd(DEAD_ZONE_KEY, (lon_f, lat_f, member))  # type: ignore[attr-defined]
            log.debug(
                "geospatial.dead_zone_added",
                truck_urn=truck_urn,
                lat=lat_f,
                lon=lon_f,
                gap_seconds=gap_seconds,
            )

    # --- query API --------------------------------------------------------

    async def trucks_near(
        self,
        lat: float,
        lon: float,
        radius_mi: float = 50.0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return trucks within ``radius_mi`` of ``(lat, lon)``, nearest first."""
        raw = await self._r.geosearch(  # type: ignore[attr-defined]
            GEO_KEY,
            longitude=lon,
            latitude=lat,
            radius=radius_mi,
            unit="mi",
            sort="ASC",
            count=limit,
            withcoord=True,
            withdist=True,
        )
        out: list[dict[str, Any]] = []
        for entry in raw or []:
            # redis-py returns [name, dist, (lon, lat)] when withcoord+withdist.
            name, dist, coord = entry[0], entry[1], entry[2]
            urn = name.decode() if isinstance(name, bytes) else name
            last_seen = await self._r.hget(SEEN_KEY, urn)  # type: ignore[attr-defined]
            if isinstance(last_seen, bytes):
                last_seen = last_seen.decode()
            out.append(
                {
                    "truck_urn": urn,
                    "distance_mi": round(float(dist), 3),
                    "lon": float(coord[0]),
                    "lat": float(coord[1]),
                    "last_seen_at": last_seen,
                }
            )
        return out

    async def truck_position(self, truck_urn: str) -> dict[str, Any] | None:
        raw = await self._r.geopos(GEO_KEY, truck_urn)  # type: ignore[attr-defined]
        if not raw or raw[0] is None:
            return None
        lon, lat = raw[0]
        last_seen = await self._r.hget(SEEN_KEY, truck_urn)  # type: ignore[attr-defined]
        if isinstance(last_seen, bytes):
            last_seen = last_seen.decode()
        return {
            "truck_urn": truck_urn,
            "lat": float(lat),
            "lon": float(lon),
            "last_seen_at": last_seen,
        }

    async def health(self) -> dict[str, Any]:
        count = await self._r.zcard(GEO_KEY)  # type: ignore[attr-defined]
        dead_zone_count = await self._r.zcard(DEAD_ZONE_KEY)  # type: ignore[attr-defined]
        return {
            "name": self.name,
            "ok": True,
            "trucks_indexed": int(count or 0),
            "dead_zones_indexed": int(dead_zone_count or 0),
        }

    async def dead_zones_near(
        self,
        lat: float,
        lon: float,
        radius_mi: float = 50.0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return dead zones within ``radius_mi`` of ``(lat, lon)``, nearest first."""
        raw = await self._r.geosearch(  # type: ignore[attr-defined]
            DEAD_ZONE_KEY,
            longitude=lon,
            latitude=lat,
            radius=radius_mi,
            unit="mi",
            sort="ASC",
            count=limit,
            withcoord=True,
            withdist=True,
        )
        out: list[dict[str, Any]] = []
        for entry in raw or []:
            name, dist, coord = entry[0], entry[1], entry[2]
            member = name.decode() if isinstance(name, bytes) else name
            # Parse member format: "truck_urn:gap_seconds"
            parts = member.split(":")
            truck_urn = parts[0] if parts else member
            gap_seconds = float(parts[1]) if len(parts) > 1 else 0
            out.append(
                {
                    "truck_urn": truck_urn,
                    "distance_mi": round(float(dist), 3),
                    "lon": float(coord[0]),
                    "lat": float(coord[1]),
                    "gap_seconds": gap_seconds,
                }
            )
        return out
