"""
Vizion API connector — covers all 7 Class I North American railways
(UP, BNSF, CSX, NS, CN, CPKC) with a single API key. No LOA required.

Get a free trial key at https://www.vizionapi.com

Set MANDALA_VIZION_API_KEY in your .env to enable this connector.
Mandala degrades gracefully if the key is absent — rail events simply
won't be emitted.
"""
from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

import httpx

from .base import IntermodalEvent, RailMilestone

logger = logging.getLogger(__name__)

VIZION_BASE_URL = "https://api.vizionapi.com/v2"
_MILESTONE_MAP = {
    "INGATE": "mandala.rail.ingate",
    "OUTGATE": "mandala.rail.outgate",
    "ARRIVAL": "mandala.rail.arrival",
    "DEPARTURE": "mandala.rail.departure",
    "AVAILABLE_PICKUP": "mandala.rail.available_for_pickup",
    "LAST_FREE_DAY": "mandala.rail.last_free_day",
}


class VizionRailProvider:
    """
    Vizion API implementation of RailProvider.

    Usage — add to .env:
        MANDALA_VIZION_API_KEY=your_key_here

    Or pass api_key directly:
        provider = VizionRailProvider(api_key="vz_live_...")
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or os.getenv("MANDALA_VIZION_API_KEY", "")
        self._client = httpx.Client(
            base_url=VIZION_BASE_URL,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "User-Agent": "mandala/0.1",
            },
            timeout=10.0,
        )

    def is_configured(self) -> bool:
        return bool(self._api_key)

    def get_intermodal_status(self, container_id: str) -> IntermodalEvent:
        """
        Fetch current intermodal status for a container from Vizion.
        Covers all Class I railways without LOA.
        """
        if not self.is_configured():
            raise RuntimeError(
                "MANDALA_VIZION_API_KEY is not set. "
                "Get a free trial at https://www.vizionapi.com and add "
                "MANDALA_VIZION_API_KEY=your_key to your .env file."
            )

        resp = self._client.get(f"/containers/{container_id}")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        return self._parse(container_id, data)

    def _parse(self, container_id: str, data: dict) -> IntermodalEvent:
        milestones = [
            RailMilestone(
                event_type=m.get("event_type", "UNKNOWN"),
                location=m.get("location", {}).get("name", ""),
                timestamp=_parse_dt(m.get("actual_time") or m.get("estimated_time")),
                timezone=m.get("location", {}).get("timezone", "UTC"),
                raw=m,
            )
            for m in data.get("milestones", [])
        ]

        lfd_raw = data.get("last_free_day")
        eta_raw = data.get("eta") or data.get("estimated_arrival")

        return IntermodalEvent(
            container_id=container_id,
            carrier_scac=data.get("carrier", {}).get("scac", ""),
            origin_ramp=data.get("origin", {}).get("name"),
            destination_ramp=data.get("destination", {}).get("name"),
            last_free_day=_parse_dt(lfd_raw) if lfd_raw else None,
            available_for_pickup=data.get("available_for_pickup", False),
            eta=_parse_dt(eta_raw) if eta_raw else None,
            milestones=milestones,
            provider="vizion",
            retrieved_at=datetime.now(tz=UTC),
        )

    def __del__(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass


def _parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=UTC)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(tz=UTC)
