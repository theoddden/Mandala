"""Dead zone materialized view.

Maintains a Redis ``GEO`` set for connectivity dead zones derived from
ping drop-offs. Enables O(log N) spatial queries for dead zones.
"""

from __future__ import annotations

from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.views.base import MaterializedView

log = structlog.get_logger(__name__)

DEAD_ZONE_KEY = "mandala:view:dead_zones"


class DeadZoneView(MaterializedView):
    name = "dead_zones"

    def __init__(self, redis: object) -> None:
        self._r = redis

    async def apply(self, event: MandalaEvent) -> None:
        if event.type != "mandala.connectivity.dead_zone":
            return

        data = event.data if isinstance(event.data, dict) else {}
        lat = data.get("last_known_lat")
        lon = data.get("last_known_lon")
        gap_seconds = data.get("gap_duration_seconds")
        truck_urn = event.subject or ""

        if lat is None or lon is None:
            return

        try:
            lon_f, lat_f = float(lon), float(lat)
        except (TypeError, ValueError):
            return

        member = f"{truck_urn}:{gap_seconds or 0}"
        await self._r.geoadd(DEAD_ZONE_KEY, (lon_f, lat_f, member))  # type: ignore[attr-defined]

    async def dead_zones_near(
        self,
        lat: float,
        lon: float,
        radius_mi: float = 50.0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
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
            parts = member.split(":")
            truck_urn = parts[0] if parts else member
            gap_seconds = float(parts[1]) if len(parts) > 1 else 0
            out.append({
                "truck_urn": truck_urn,
                "distance_mi": round(float(dist), 3),
                "lon": float(coord[0]),
                "lat": float(coord[1]),
                "gap_seconds": gap_seconds,
            })
        return out

    async def health(self) -> dict[str, Any]:
        count = await self._r.zcard(DEAD_ZONE_KEY)  # type: ignore[attr-defined]
        return {"name": self.name, "ok": True, "dead_zones_indexed": int(count or 0)}
