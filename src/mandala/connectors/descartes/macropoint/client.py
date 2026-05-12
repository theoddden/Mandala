"""Outbound HTTP client for sending location updates back to MacroPoint.

Carriers integrating with MacroPoint POST location updates to a published
endpoint authenticated with an API key. This client is the bridge from
Mandala's normalized truck-position events back to MacroPoint's expected
``LocationUpdate`` schema.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from mandala.settings import get_settings


class MacroPointClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        s = get_settings()
        self._api_key = api_key or s.descartes_api_key
        self._base = (base_url or s.descartes_macropoint_base_url).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "mandala/0.1 (+https://github.com/mandala-bridge/mandala)",
            },
        )

    async def __aenter__(self) -> MacroPointClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.request(method, path, **kwargs)
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(f"transient {resp.status_code}", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    async def send_location_update(
        self,
        *,
        shipment_id: str,
        latitude: float,
        longitude: float,
        captured_at: datetime,
        speed_mps: float | None = None,
        heading_deg: float | None = None,
        eta: datetime | None = None,
        status: str = "InTransit",
    ) -> dict[str, Any]:
        """Send a ``LocationUpdate`` for a tracked shipment."""
        body: dict[str, Any] = {
            "MessageType": "LocationUpdate",
            "Body": {
                "ShipmentId": shipment_id,
                "Latitude": latitude,
                "Longitude": longitude,
                "Timestamp": captured_at.isoformat(),
                "Status": status,
            },
        }
        if speed_mps is not None:
            body["Body"]["SpeedMps"] = speed_mps
        if heading_deg is not None:
            body["Body"]["Heading"] = heading_deg
        if eta is not None:
            body["Body"]["Eta"] = eta.isoformat()

        resp = await self._request("POST", "/v1/location-updates", json=body)
        return resp.json() if resp.content else {"ok": True}
