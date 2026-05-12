"""Graph materialized view (RedisGraph / FalkorDB).

Projects relationships between canonical entities into a property graph:

    (Truck {urn})-[:HAULS]->(Shipment {urn})
    (Shipment {urn})-[:FILED_WITH]->(Authority {code})
    (Shipment {urn})-[:CROSSED]->(POE {id})

Enables multi-hop queries ("which shipper's goods have passed through
Laredo with a customs hold in the last 30 days?") that a KV projection
can't answer without scanning.

**Optional dependency.** RedisGraph was deprecated in Redis 7.4 and lives
on as `FalkorDB <https://www.falkordb.com/>`_. On startup we probe
``MODULE LIST`` for either module; if neither is loaded, the view logs a
single warning and becomes a no-op. This is by design — graph is
high-value but low-footprint and shouldn't block the other views.
"""
from __future__ import annotations

from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType
from mandala.views.base import MaterializedView

log = structlog.get_logger(__name__)

GRAPH_NAME = "mandala"
_SUPPORTED_MODULES = {"graph", "falkordb"}


class GraphView(MaterializedView):
    name = "graph"

    def __init__(self, redis: object) -> None:
        self._r = redis
        self._available: bool | None = None  # lazy probe

    async def _probe(self) -> bool:
        if self._available is not None:
            return self._available
        try:
            modules = await self._r.execute_command("MODULE", "LIST")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            self._available = False
            log.warning("graph.view.disabled", reason="MODULE LIST failed")
            return False

        # MODULE LIST returns a list of lists like [[b'name', b'graph', b'ver', 20812], ...]
        found = False
        for mod in modules or []:
            try:
                # redis-py returns flat pairs; normalise both forms.
                if isinstance(mod, (list, tuple)):
                    # extract the value after b'name'
                    for i, item in enumerate(mod):
                        key = item.decode() if isinstance(item, bytes) else item
                        if key == "name" and i + 1 < len(mod):
                            name = mod[i + 1]
                            name_s = name.decode() if isinstance(name, bytes) else name
                            if name_s.lower() in _SUPPORTED_MODULES:
                                found = True
                                break
            except Exception:  # noqa: BLE001
                continue
            if found:
                break
        self._available = found
        if not found:
            log.warning(
                "graph.view.disabled",
                reason="neither RedisGraph nor FalkorDB module loaded",
            )
        return found

    async def _query(self, cypher: str, params: dict[str, Any] | None = None) -> Any:
        # FalkorDB / RedisGraph accept GRAPH.QUERY + params.
        args = ["GRAPH.QUERY", GRAPH_NAME, cypher]
        if params:
            # Inline params as CYPHER parameters header (simplest portable form).
            param_str = "CYPHER " + " ".join(f"{k}={_fmt_param(v)}" for k, v in params.items())
            args = ["GRAPH.QUERY", GRAPH_NAME, f"{param_str} {cypher}"]
        return await self._r.execute_command(*args)  # type: ignore[attr-defined]

    async def apply(self, event: MandalaEvent) -> None:
        if not await self._probe():
            return
        try:
            await self._apply_inner(event)
        except Exception:  # noqa: BLE001
            log.exception("graph.view.apply_failed", event_id=event.id, type=event.type)

    async def _apply_inner(self, event: MandalaEvent) -> None:
        data = event.data if isinstance(event.data, dict) else {}

        if event.type == EventType.SHIPMENT_HANDOFF.value:
            truck_urn = data.get("truck_urn")
            shipment_urn = data.get("shipment_urn") or event.subject
            if truck_urn and shipment_urn:
                await self._query(
                    "MERGE (t:Truck {urn: $t}) "
                    "MERGE (s:Shipment {urn: $s}) "
                    "MERGE (t)-[:HAULS]->(s)",
                    {"t": truck_urn, "s": shipment_urn},
                )
            return

        if event.type == EventType.CUSTOMS_FILED.value:
            shipment_urn = event.subject
            authority = data.get("authority") or "unknown"
            if shipment_urn:
                await self._query(
                    "MERGE (s:Shipment {urn: $s}) "
                    "MERGE (a:Authority {code: $a}) "
                    "MERGE (s)-[:FILED_WITH]->(a)",
                    {"s": shipment_urn, "a": str(authority)},
                )
            return

        if event.type == EventType.TRUCK_GEOFENCE_ENTERED.value:
            truck_urn = event.subject
            poe = data.get("geofence_id") or data.get("geofence_name")
            if truck_urn and poe:
                await self._query(
                    "MERGE (t:Truck {urn: $t}) "
                    "MERGE (p:POE {id: $p}) "
                    "MERGE (t)-[:CROSSED]->(p)",
                    {"t": truck_urn, "p": str(poe)},
                )
            return

    # --- query API --------------------------------------------------------

    async def neighbors(self, urn: str, depth: int = 2, limit: int = 50) -> list[dict[str, Any]]:
        if not await self._probe():
            return []
        # Depth is embedded in the query, not a parameter (GRAPH.QUERY
        # doesn't support variable path lengths via bound params).
        d = max(1, min(int(depth), 5))
        cypher = (
            f"MATCH (n {{urn: $urn}})-[*1..{d}]-(m) "
            "RETURN DISTINCT labels(m) AS labels, m.urn AS urn "
            f"LIMIT {int(limit)}"
        )
        raw = await self._query(cypher, {"urn": urn})
        return _decode_graph_result(raw)

    async def health(self) -> dict[str, Any]:
        available = await self._probe()
        out: dict[str, Any] = {"name": self.name, "ok": available, "module_loaded": available}
        if available:
            try:
                raw = await self._query("MATCH (n) RETURN count(n) AS c")
                rows = _decode_graph_result(raw)
                out["node_count"] = rows[0].get("c") if rows else 0
            except Exception:  # noqa: BLE001
                out["ok"] = False
        return out


def _fmt_param(v: Any) -> str:
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    return str(v)


def _decode_graph_result(raw: Any) -> list[dict[str, Any]]:
    """Decode a GRAPH.QUERY response into a list of row dicts.

    Response format is ``[header, rows, statistics]`` where ``header`` is a
    list of column descriptors and ``rows`` is a list of lists.
    """
    if not raw or len(raw) < 2:
        return []
    header = raw[0]
    rows = raw[1]
    col_names: list[str] = []
    for h in header or []:
        # Each header entry is [type, name] in RedisGraph responses.
        if isinstance(h, (list, tuple)) and len(h) >= 2:
            name = h[1]
        else:
            name = h
        col_names.append(name.decode() if isinstance(name, bytes) else str(name))

    out: list[dict[str, Any]] = []
    for row in rows or []:
        d: dict[str, Any] = {}
        for i, col in enumerate(col_names):
            v = row[i] if i < len(row) else None
            if isinstance(v, bytes):
                v = v.decode()
            d[col] = v
        out.append(d)
    return out
