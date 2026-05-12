"""Cold-chain time-series materialized view.

Two sorted sets per truck:

* ``mandala:view:ts:cold:<truck_urn>`` — every temperature reading in the
  retention window. Score = epoch seconds, member = ``<temp_c>|<event_id>``.
* ``mandala:view:ts:breaches`` — global index of cold-chain breach events
  across the whole fleet (score = epoch seconds, member = ``<truck_urn>|<event_id>``).

Retention is bounded by ``settings.views_timeseries_ttl_seconds``: a
``ZREMRANGEBYSCORE`` trim runs on every apply to drop readings older than
``now - ttl``. This keeps memory bounded without needing Redis key TTLs
(which don't apply to individual sorted-set members).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.events.types import EventType
from mandala.settings import get_settings
from mandala.views.base import MaterializedView

log = structlog.get_logger(__name__)

READINGS_KEY_PREFIX = "mandala:view:ts:cold:"
BREACHES_KEY = "mandala:view:ts:breaches"


def _readings_key(truck_urn: str) -> str:
    return f"{READINGS_KEY_PREFIX}{truck_urn}"


def _ts_epoch(event: MandalaEvent) -> float:
    return (event.time or datetime.now(UTC)).timestamp()


class TimeseriesView(MaterializedView):
    name = "timeseries"

    COLD_EVENT_TYPES = {
        EventType.COLD_CHAIN_READING.value,
        EventType.COLD_CHAIN_BREACH.value,
        EventType.COLD_CHAIN_RECOVERED.value,
    }

    def __init__(self, redis: object) -> None:
        self._r = redis

    async def apply(self, event: MandalaEvent) -> None:
        if event.type not in self.COLD_EVENT_TYPES:
            return
        data = event.data if isinstance(event.data, dict) else {}
        truck_urn = event.subject or data.get("truck_urn") or ""
        if not truck_urn.startswith("urn:mandala:truck:"):
            return

        temp = data.get("temperature_c")
        if temp is None:
            return
        try:
            temp_f = float(temp)
        except (TypeError, ValueError):
            return

        score = _ts_epoch(event)
        member = f"{temp_f}|{event.id}"

        # Append reading; ZADD with identical (score, member) is a no-op → idempotent.
        await self._r.zadd(_readings_key(truck_urn), {member: score})  # type: ignore[attr-defined]

        if event.type == EventType.COLD_CHAIN_BREACH.value:
            await self._r.zadd(  # type: ignore[attr-defined]
                BREACHES_KEY,
                {f"{truck_urn}|{event.id}": score},
            )

        # Retention trim (best-effort, don't fail the apply if it errors).
        ttl = get_settings().views_timeseries_ttl_seconds
        cutoff = score - ttl
        try:
            await self._r.zremrangebyscore(_readings_key(truck_urn), "-inf", cutoff)  # type: ignore[attr-defined]
            await self._r.zremrangebyscore(BREACHES_KEY, "-inf", cutoff)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            log.debug("timeseries.retention_trim_failed", truck=truck_urn)

    # --- query API --------------------------------------------------------

    async def range(
        self,
        truck_urn: str,
        since_epoch: float,
        until_epoch: float,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        raw = await self._r.zrangebyscore(  # type: ignore[attr-defined]
            _readings_key(truck_urn),
            since_epoch,
            until_epoch,
            withscores=True,
            start=0,
            num=limit,
        )
        out: list[dict[str, Any]] = []
        for member, score in raw or []:
            if isinstance(member, bytes):
                member = member.decode()
            temp_s, _, event_id = member.partition("|")
            try:
                temp_c = float(temp_s)
            except ValueError:
                continue
            out.append(
                {
                    "at": datetime.fromtimestamp(float(score), UTC).isoformat(),
                    "temperature_c": temp_c,
                    "event_id": event_id,
                }
            )
        return out

    async def recent_breaches(
        self,
        since_epoch: float,
        until_epoch: float | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        until = until_epoch if until_epoch is not None else datetime.now(UTC).timestamp()
        raw = await self._r.zrangebyscore(  # type: ignore[attr-defined]
            BREACHES_KEY,
            since_epoch,
            until,
            withscores=True,
            start=0,
            num=limit,
        )
        out: list[dict[str, Any]] = []
        for member, score in raw or []:
            if isinstance(member, bytes):
                member = member.decode()
            truck_urn, _, event_id = member.partition("|")
            out.append(
                {
                    "truck_urn": truck_urn,
                    "event_id": event_id,
                    "at": datetime.fromtimestamp(float(score), UTC).isoformat(),
                }
            )
        return out

    async def health(self) -> dict[str, Any]:
        breaches = await self._r.zcard(BREACHES_KEY)  # type: ignore[attr-defined]
        return {"name": self.name, "ok": True, "breaches_indexed": int(breaches or 0)}
