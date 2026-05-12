"""Unit tests for materialized views.

These tests use a minimal in-memory fake Redis that implements exactly the
commands the views use — no external Redis required. Real integration tests
against a live Redis / FalkorDB belong in a separate suite.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from math import asin, cos, radians, sin, sqrt
from typing import Any

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.views.bitmap import BitmapView
from mandala.views.geospatial import GeospatialView
from mandala.views.timeseries import TimeseriesView

# -----------------------------------------------------------------------------
# Minimal fake async Redis — just enough for the view implementations.
# -----------------------------------------------------------------------------


class FakeRedis:
    def __init__(self) -> None:
        self._kv: dict[str, Any] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sets: dict[str, set[str]] = {}
        self._zsets: dict[str, dict[str, float]] = {}
        self._geo: dict[str, dict[str, tuple[float, float]]] = {}
        self._bitmaps: dict[str, bytearray] = {}

    # --- KV
    async def get(self, key: str) -> bytes | None:
        v = self._kv.get(key)
        if v is None and key in self._bitmaps:
            return bytes(self._bitmaps[key])
        if v is None:
            return None
        return v.encode() if isinstance(v, str) else v

    async def set(self, key: str, value: Any, **_: Any) -> bool:
        self._kv[key] = value
        return True

    async def delete(self, *keys: str) -> int:
        n = 0
        for k in keys:
            for bucket in (self._kv, self._hashes, self._sets, self._zsets, self._geo, self._bitmaps):
                if k in bucket:
                    del bucket[k]
                    n += 1
        return n

    async def incr(self, key: str) -> int:
        self._kv[key] = int(self._kv.get(key, 0)) + 1
        return self._kv[key]

    # --- Hash
    async def hget(self, name: str, key: str) -> bytes | None:
        v = self._hashes.get(name, {}).get(str(key))
        return v.encode() if isinstance(v, str) else v

    async def hset(self, name: str, key: str | None = None, value: Any = None, mapping: dict | None = None) -> int:
        self._hashes.setdefault(name, {})
        if mapping:
            for k, v in mapping.items():
                self._hashes[name][str(k)] = str(v)
            return len(mapping)
        self._hashes[name][str(key)] = str(value)
        return 1

    async def hmget(self, name: str, *keys: str) -> list[bytes | None]:
        h = self._hashes.get(name, {})
        out: list[bytes | None] = []
        for k in keys:
            v = h.get(str(k))
            out.append(v.encode() if isinstance(v, str) else v)
        return out

    async def hsetnx(self, name: str, key: str, value: Any) -> int:
        self._hashes.setdefault(name, {})
        if str(key) in self._hashes[name]:
            return 0
        self._hashes[name][str(key)] = str(value)
        return 1

    # --- Set
    async def sadd(self, key: str, *members: str) -> int:
        self._sets.setdefault(key, set())
        before = len(self._sets[key])
        self._sets[key].update(str(m) for m in members)
        return len(self._sets[key]) - before

    async def smembers(self, key: str) -> set[bytes]:
        return {m.encode() for m in self._sets.get(key, set())}

    async def scard(self, key: str) -> int:
        return len(self._sets.get(key, set()))

    # --- Sorted set
    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        zs = self._zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in zs:
                added += 1
            zs[member] = score
        return added

    async def zcard(self, key: str) -> int:
        # Real Redis GEO sets are sorted sets, so ZCARD works on GEO keys.
        if key in self._geo:
            return len(self._geo[key])
        return len(self._zsets.get(key, {}))

    async def zrangebyscore(
        self, key: str, min_: float, max_: float, withscores: bool = False, start: int = 0, num: int = 1000
    ) -> list:
        items = sorted(self._zsets.get(key, {}).items(), key=lambda kv: kv[1])
        filtered = [(m, s) for m, s in items if min_ <= s <= max_]
        sliced = filtered[start : start + num]
        if withscores:
            return [(m.encode(), s) for m, s in sliced]
        return [m.encode() for m, _ in sliced]

    async def zremrangebyscore(self, key: str, min_: Any, max_: float) -> int:
        zs = self._zsets.get(key, {})
        if not zs:
            return 0
        to_remove = [m for m, s in zs.items() if (min_ == "-inf" or s >= float(min_)) and s <= max_]
        for m in to_remove:
            del zs[m]
        return len(to_remove)

    # --- Geo
    async def geoadd(self, key: str, tuple_arg: tuple[float, float, str]) -> int:
        lon, lat, name = tuple_arg
        self._geo.setdefault(key, {})
        new = name not in self._geo[key]
        self._geo[key][name] = (lon, lat)
        return 1 if new else 0

    async def geopos(self, key: str, name: str) -> list:
        entry = self._geo.get(key, {}).get(name)
        if not entry:
            return [None]
        return [entry]

    async def geosearch(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "mi",
        sort: str = "ASC",
        count: int = 100,
        withcoord: bool = False,
        withdist: bool = False,
    ) -> list:
        out = []
        for name, (lon, lat) in self._geo.get(key, {}).items():
            d_km = _haversine_km(latitude, longitude, lat, lon)
            d = d_km * 0.621371 if unit == "mi" else d_km
            if d <= radius:
                out.append((name.encode(), d, (lon, lat)))
        out.sort(key=lambda e: e[1])
        return out[:count]

    # --- Bitmap
    async def setbit(self, key: str, offset: int, value: int) -> int:
        ba = self._bitmaps.setdefault(key, bytearray())
        byte_idx = offset // 8
        bit_idx = 7 - (offset % 8)
        while len(ba) <= byte_idx:
            ba.append(0)
        prev = (ba[byte_idx] >> bit_idx) & 1
        if value:
            ba[byte_idx] |= 1 << bit_idx
        else:
            ba[byte_idx] &= ~(1 << bit_idx) & 0xFF
        return prev

    async def bitop(self, op: str, dest: str, *sources: str) -> int:
        srcs = [self._bitmaps.get(s, bytearray()) for s in sources]
        max_len = max((len(s) for s in srcs), default=0)
        out = bytearray(max_len)
        for i in range(max_len):
            bytes_ = [s[i] if i < len(s) else 0 for s in srcs]
            if op == "NOT":
                out[i] = (~bytes_[0]) & 0xFF
            elif op == "AND":
                v = 0xFF
                for b in bytes_:
                    v &= b
                out[i] = v
        self._bitmaps[dest] = out
        return max_len

    # --- Lua
    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        # Only supports the get-or-create integer id pattern used in bitmap.py.
        id_map_key, id_seq_key, reverse_map_key = args[0], args[1], args[2]
        urn = args[3]
        self._hashes.setdefault(id_map_key, {})
        if urn in self._hashes[id_map_key]:
            return int(self._hashes[id_map_key][urn])
        new_id = int(self._kv.get(id_seq_key, 0)) + 1
        self._kv[id_seq_key] = new_id
        self._hashes[id_map_key][urn] = str(new_id)
        self._hashes.setdefault(reverse_map_key, {})[str(new_id)] = urn
        return new_id


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


# -----------------------------------------------------------------------------
# Geospatial view
# -----------------------------------------------------------------------------


def _truck_position_event(urn: str, lat: float, lon: float) -> MandalaEvent:
    return new_event(
        type=EventType.TRUCK_POSITION,
        source="mandala/test",
        subject=urn,
        data={"position": {"point": {"lat": lat, "lon": lon}}},
    )


def test_geospatial_view_indexes_and_queries_positions() -> None:
    async def _run() -> None:
        r = FakeRedis()
        view = GeospatialView(r)

        # Laredo, TX (27.5, -99.5) ± a few trucks nearby and one far away.
        await view.apply(_truck_position_event("urn:mandala:truck:samsara:A", 27.52, -99.50))
        await view.apply(_truck_position_event("urn:mandala:truck:samsara:B", 27.60, -99.40))
        await view.apply(_truck_position_event("urn:mandala:truck:samsara:C", 40.71, -74.00))  # NYC

        near = await view.trucks_near(lat=27.5036, lon=-99.5076, radius_mi=30.0)
        urns = [t["truck_urn"] for t in near]
        assert "urn:mandala:truck:samsara:A" in urns
        assert "urn:mandala:truck:samsara:B" in urns
        assert "urn:mandala:truck:samsara:C" not in urns
        # Sorted nearest-first.
        assert near[0]["distance_mi"] <= near[-1]["distance_mi"]

    asyncio.run(_run())


def test_geospatial_view_idempotent_reapply() -> None:
    async def _run() -> None:
        r = FakeRedis()
        view = GeospatialView(r)
        evt = _truck_position_event("urn:mandala:truck:samsara:A", 27.5, -99.5)
        await view.apply(evt)
        await view.apply(evt)  # second apply must not corrupt state
        health = await view.health()
        assert health["trucks_indexed"] == 1

    asyncio.run(_run())


def test_geospatial_view_ignores_non_position_events() -> None:
    async def _run() -> None:
        r = FakeRedis()
        view = GeospatialView(r)
        evt = new_event(
            type=EventType.SHIPMENT_DELIVERED,
            source="mandala/test",
            subject="urn:mandala:shipment:foo",
            data={},
        )
        await view.apply(evt)
        health = await view.health()
        assert health["trucks_indexed"] == 0

    asyncio.run(_run())


# -----------------------------------------------------------------------------
# Timeseries view
# -----------------------------------------------------------------------------


def _cold_reading_event(urn: str, temp_c: float, event_type: EventType = EventType.COLD_CHAIN_READING) -> MandalaEvent:
    return new_event(
        type=event_type,
        source="mandala/test",
        subject=urn,
        data={"temperature_c": temp_c},
    )


def test_timeseries_view_records_readings_and_breaches() -> None:
    async def _run() -> None:
        r = FakeRedis()
        view = TimeseriesView(r)
        urn = "urn:mandala:truck:samsara:A"

        await view.apply(_cold_reading_event(urn, 3.5))
        await view.apply(_cold_reading_event(urn, 12.0, EventType.COLD_CHAIN_BREACH))

        now = datetime.now(UTC).timestamp()
        readings = await view.range(urn, since_epoch=now - 3600, until_epoch=now + 3600)
        assert len(readings) == 2
        breaches = await view.recent_breaches(since_epoch=now - 3600, until_epoch=now + 3600)
        assert len(breaches) == 1
        assert breaches[0]["truck_urn"] == urn

    asyncio.run(_run())


def test_timeseries_view_rejects_unparseable_temp() -> None:
    async def _run() -> None:
        r = FakeRedis()
        view = TimeseriesView(r)
        evt = new_event(
            type=EventType.COLD_CHAIN_READING,
            source="mandala/test",
            subject="urn:mandala:truck:samsara:A",
            data={"temperature_c": "not-a-number"},
        )
        await view.apply(evt)
        health = await view.health()
        assert health["breaches_indexed"] == 0

    asyncio.run(_run())


# -----------------------------------------------------------------------------
# Bitmap view
# -----------------------------------------------------------------------------


def test_bitmap_view_at_poe_without_filing() -> None:
    async def _run() -> None:
        r = FakeRedis()
        # No StateStore interaction required for this path — pass a stub.
        from mandala.core.state import StateStore

        view = BitmapView(r, StateStore(r))
        poe = "us-mx:laredo-tx"
        urn_a = "urn:mandala:truck:samsara:A"
        urn_b = "urn:mandala:truck:samsara:B"

        # Both trucks enter the POE.
        for urn in (urn_a, urn_b):
            evt = new_event(
                type=EventType.TRUCK_GEOFENCE_ENTERED,
                source="mandala/test",
                subject=urn,
                data={"geofence_id": poe, "geofence_name": "Laredo Border POE"},
            )
            await view.apply(evt)

        # A has its link and customs filed; B has nothing.
        await r.set("mandala:link:shipment:urn:mandala:shipment:sA", urn_a)
        # Use StateStore to register the link so the CUSTOMS_FILED handler finds it.
        await r.set("mandala:link:shipment:urn:mandala:shipment:sA", urn_a)
        filed = new_event(
            type=EventType.CUSTOMS_FILED,
            source="mandala/test",
            subject="urn:mandala:shipment:sA",
            data={"authority": "CBP"},
        )
        await view.apply(filed)

        no_filing = await view.at_poe_without_filing(poe)
        # B should still be flagged (no filing); A should not (filed set to 1).
        assert urn_b in no_filing
        assert urn_a not in no_filing

    asyncio.run(_run())


def test_bitmap_view_truck_offset_is_stable() -> None:
    async def _run() -> None:
        r = FakeRedis()
        from mandala.core.state import StateStore

        view = BitmapView(r, StateStore(r))
        urn = "urn:mandala:truck:samsara:A"
        a = await view._truck_offset(urn)
        b = await view._truck_offset(urn)
        c = await view._truck_offset("urn:mandala:truck:samsara:B")
        assert a == b
        assert a != c

    asyncio.run(_run())
