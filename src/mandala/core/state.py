"""Tiny Redis-backed projection of canonical entities.

Mandala does **not** own the source of truth — it remembers just enough
to answer "do we know of a customs filing for this shipment?" and "which
shipment is this truck linked to?". Everything is TTL'd.

To support clearing fields (e.g., resetting ``customs_status`` from
``hold`` to ``None``), pass the sentinel value ``STATE_DELETE`` as the
value in the patch — the key will be removed from the stored object.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from mandala.settings import get_settings


def _key(*parts: str) -> str:
    return ":".join(("mandala", *parts))


# Sentinel value used to delete a key from a state object.
STATE_DELETE = object()


class StateStore:
    def __init__(self, redis: object, ttl_seconds: int | None = None) -> None:
        self._r = redis
        self._ttl = ttl_seconds or get_settings().state_ttl_seconds

    async def _get_json(self, k: str) -> dict[str, Any] | None:
        raw = await self._r.get(k)  # type: ignore[attr-defined]
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)

    async def _set_json(self, k: str, value: dict[str, Any]) -> None:
        value["updated_at"] = datetime.now(UTC).isoformat()
        await self._r.set(k, json.dumps(value, default=str), ex=self._ttl)  # type: ignore[attr-defined]

    # --- shipments / trucks ----------------------------------------------
    async def upsert(self, kind: str, urn: str, patch: dict[str, Any]) -> None:
        existing = await self._get_json(_key(kind, urn)) or {}
        for k, v in patch.items():
            if v is STATE_DELETE:
                existing.pop(k, None)
            elif v is not None:
                existing[k] = v
        await self._set_json(_key(kind, urn), existing)

    async def get(self, kind: str, urn: str) -> dict[str, Any] | None:
        return await self._get_json(_key(kind, urn))

    # --- truck <-> shipment links ----------------------------------------
    async def link(self, truck_urn: str, shipment_urn: str) -> None:
        await self._r.set(_key("link", "truck", truck_urn), shipment_urn, ex=self._ttl)  # type: ignore[attr-defined]
        await self._r.set(_key("link", "shipment", shipment_urn), truck_urn, ex=self._ttl)  # type: ignore[attr-defined]

    async def shipment_for_truck(self, truck_urn: str) -> str | None:
        v = await self._r.get(_key("link", "truck", truck_urn))  # type: ignore[attr-defined]
        return v.decode() if isinstance(v, bytes) else v

    # --- timeline (capped list) -------------------------------------------
    async def append_timeline(self, shipment_urn: str, entry: dict[str, Any]) -> None:
        k = _key("timeline", shipment_urn)
        await self._r.rpush(k, json.dumps(entry, default=str))  # type: ignore[attr-defined]
        await self._r.expire(k, self._ttl)  # type: ignore[attr-defined]
        await self._r.ltrim(k, -1000, -1)  # type: ignore[attr-defined]

    async def read_timeline(self, shipment_urn: str) -> list[dict[str, Any]]:
        raw = await self._r.lrange(_key("timeline", shipment_urn), 0, -1)  # type: ignore[attr-defined]
        return [json.loads(x.decode() if isinstance(x, bytes) else x) for x in raw or []]
