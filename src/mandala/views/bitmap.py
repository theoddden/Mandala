"""Port-of-Entry bitmap materialized view.

Answers the single most important cross-system question Mandala exists to
answer — **"which trucks are at a border POE right now, and which of those
don't yet have a released customs filing?"** — in O(bitmap-size / 8) via
boolean set algebra instead of O(trucks × POEs) scans.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType
from mandala.core.state import StateStore
from mandala.views.base import MaterializedView

# Try to import Rust-accelerated bitmap operations
try:
    from mandala_rust_ext import bitmap_extract_offsets

    _RUST_BITMAP_AVAILABLE = True
except ImportError:
    _RUST_BITMAP_AVAILABLE = False

log = structlog.get_logger(__name__)

ID_MAP_KEY = "mandala:view:bm:urn_to_id"
REVERSE_MAP_KEY = "mandala:view:bm:id_to_urn"
ID_SEQ_KEY = "mandala:view:bm:id_seq"

# Atomic get-or-create integer id for a truck URN. Returns the integer.
GET_OR_CREATE_ID_SCRIPT = """
local id = redis.call('HGET', KEYS[1], ARGV[1])
if id then return tonumber(id) end
local new_id = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], ARGV[1], new_id)
redis.call('HSET', KEYS[3], new_id, ARGV[1])
return new_id
"""

_BORDER_TAGS = {"border_poe", "us-mx", "us-ca", "border", "poe"}


def _is_border_poe(data: dict) -> bool:
    name = str(data.get("geofence_name") or "").lower()
    gid = str(data.get("geofence_id") or "").lower()
    return any(t in name or t in gid for t in _BORDER_TAGS)


def _poe_key(poe: str) -> str:
    # Normalise: prefer a stable geofence id over the human name.
    return poe.lower().replace(" ", "-").replace(":", "-")


def _present_key(poe: str) -> str:
    return f"mandala:view:bm:poe:{_poe_key(poe)}:present"


def _filed_key(poe: str) -> str:
    return f"mandala:view:bm:poe:{_poe_key(poe)}:filed"


POE_INDEX_KEY = "mandala:view:bm:poe_index"


class BitmapView(MaterializedView):
    name = "bitmap"

    def __init__(self, redis: object, state: StateStore | None = None) -> None:
        self._r = redis
        self._state = state or StateStore(redis)

    async def _truck_offset(self, truck_urn: str) -> int:
        raw = await self._r.eval(  # type: ignore[attr-defined]
            GET_OR_CREATE_ID_SCRIPT,
            3,
            ID_MAP_KEY,
            ID_SEQ_KEY,
            REVERSE_MAP_KEY,
            truck_urn,
        )
        return int(raw)

    async def apply(self, event: MandalaEvent) -> None:  # noqa: C901
        data = event.data if isinstance(event.data, dict) else {}

        # --- Geofence enter/exit on POE ---------------------------------
        if event.type == EventType.TRUCK_GEOFENCE_ENTERED.value and _is_border_poe(data):
            truck_urn = event.subject or ""
            if not truck_urn.startswith("urn:mandala:truck:"):
                return
            poe = data.get("geofence_id") or data.get("geofence_name") or "unknown"
            offset = await self._truck_offset(truck_urn)
            await self._r.setbit(_present_key(poe), offset, 1)  # type: ignore[attr-defined]
            await self._r.sadd(POE_INDEX_KEY, str(poe))  # type: ignore[attr-defined]

            # Seed the filed bitmap from state so we don't have to wait for
            # the next customs event.
            filed = await self._truck_is_filed(truck_urn)
            await self._r.setbit(_filed_key(poe), offset, 1 if filed else 0)  # type: ignore[attr-defined]
            return

        if event.type == EventType.TRUCK_GEOFENCE_EXITED.value and _is_border_poe(data):
            truck_urn = event.subject or ""
            if not truck_urn.startswith("urn:mandala:truck:"):
                return
            poe = data.get("geofence_id") or data.get("geofence_name") or "unknown"
            offset = await self._truck_offset(truck_urn)
            await self._r.setbit(_present_key(poe), offset, 0)  # type: ignore[attr-defined]
            # Leave the filed bitmap bit alone — it's reset next entry.
            return

        # --- Customs status transitions ---------------------------------
        customs_events = {
            EventType.CUSTOMS_FILED.value: 1,
            EventType.CUSTOMS_RELEASED.value: 1,
            EventType.CUSTOMS_HOLD.value: 0,
            EventType.CUSTOMS_EXAM.value: 0,
            EventType.CUSTOMS_REJECTED.value: 0,
        }
        if event.type in customs_events:
            shipment_urn = event.subject or ""
            if not shipment_urn.startswith("urn:mandala:shipment:"):
                return
            # Reverse-link: shipment → truck
            truck_urn_raw = await self._r.get(f"mandala:link:shipment:{shipment_urn}")  # type: ignore[attr-defined]
            if not truck_urn_raw:
                return
            truck_urn = truck_urn_raw.decode() if isinstance(truck_urn_raw, bytes) else truck_urn_raw

            offset = await self._truck_offset(truck_urn)
            bit = customs_events[event.type]
            # Update the bit at every POE the truck is known to have been seen
            # at. In practice, a truck is present at one POE at a time, but
            # we don't know which — so we iterate the small POE index.
            poes = await self._r.smembers(POE_INDEX_KEY)  # type: ignore[attr-defined]
            for poe in poes or []:
                poe_s = poe.decode() if isinstance(poe, bytes) else poe
                await self._r.setbit(_filed_key(poe_s), offset, bit)  # type: ignore[attr-defined]

    async def _truck_is_filed(self, truck_urn: str) -> bool:
        shipment_urn = await self._state.shipment_for_truck(truck_urn)
        if not shipment_urn:
            return False
        shipment = await self._state.get("shipment", shipment_urn) or {}
        return shipment.get("customs_status") in ("filed", "released")

    # --- query API --------------------------------------------------------

    async def at_poe(self, poe: str) -> list[str]:
        return await self._bitmap_urns(_present_key(poe))

    async def at_poe_without_filing(self, poe: str) -> list[str]:
        """Bitmap AND NOT — trucks present at POE whose filing bit is 0."""
        # Use a temporary destination key to hold the AND NOT result.
        # Append a random suffix to avoid key collisions on concurrent queries.
        suffix = uuid.uuid4().hex[:8]
        tmp_key = f"mandala:view:bm:tmp:{_poe_key(poe)}:no_filing:{suffix}"
        # BITOP does not support NOT + AND in one call. Two steps:
        # 1) not_filed = NOT filed
        # 2) present_without_filing = present AND not_filed
        not_filed_key = f"mandala:view:bm:tmp:{_poe_key(poe)}:not_filed:{suffix}"
        try:
            await self._r.bitop("NOT", not_filed_key, _filed_key(poe))  # type: ignore[attr-defined]
            await self._r.bitop("AND", tmp_key, _present_key(poe), not_filed_key)  # type: ignore[attr-defined]
            return await self._bitmap_urns(tmp_key)
        finally:
            await self._r.delete(tmp_key, not_filed_key)  # type: ignore[attr-defined]

    async def _bitmap_urns(self, bitmap_key: str) -> list[str]:
        raw = await self._r.get(bitmap_key)  # type: ignore[attr-defined]
        if not raw:
            return []
        if isinstance(raw, str):
            raw = raw.encode("latin-1")

        # Use Rust-accelerated bitmap extraction if available
        if _RUST_BITMAP_AVAILABLE:
            offsets = bitmap_extract_offsets(raw)
        else:
            # Pure Python fallback
            offsets: list[int] = []
            for byte_idx, byte in enumerate(raw):
                if byte == 0:
                    continue
                for bit in range(8):
                    if byte & (1 << (7 - bit)):
                        offsets.append(byte_idx * 8 + bit)

        if not offsets:
            return []
        urns = await self._r.hmget(REVERSE_MAP_KEY, *[str(o) for o in offsets])  # type: ignore[attr-defined]
        out: list[str] = []
        for urn in urns or []:
            if urn is None:
                continue
            out.append(urn.decode() if isinstance(urn, bytes) else urn)
        return out

    async def health(self) -> dict[str, Any]:
        seq = await self._r.get(ID_SEQ_KEY)  # type: ignore[attr-defined]
        poes = await self._r.scard(POE_INDEX_KEY)  # type: ignore[attr-defined]
        return {
            "name": self.name,
            "ok": True,
            "truck_ids_assigned": int(seq) if seq else 0,
            "poes_tracked": int(poes or 0),
        }
