"""Async outbound client for the Samsara REST API.

Used for:
* polling (when webhooks aren't available),
* fleet enrichment (looking up vehicle / driver metadata),
* outbound actions from playbooks (sending driver alerts, dispatching).

Only a small documented subset is wrapped; the underlying ``request()``
method exposes the full surface for advanced usage.

Reference: https://developers.samsara.com/reference
"""
from __future__ import annotations

from typing import Any, AsyncIterator

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class SamsaraClient:
    """Thin async wrapper over Samsara REST."""

    def __init__(
        self,
        token: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        s = get_settings()
        self._token = token or s.samsara_api_token
        self._base = (base_url or s.samsara_base_url).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "User-Agent": "mandala/0.1 (+https://github.com/mandala-bridge/mandala)",
            },
        )

    async def __aenter__(self) -> "SamsaraClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request with retries on transient failures."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.request(method, path, **kwargs)
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"transient {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    # --- Convenience ------------------------------------------------------

    async def list_vehicles(self) -> AsyncIterator[dict[str, Any]]:
        """Iterate every vehicle, transparently handling cursor pagination."""
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 100}
            if cursor:
                params["after"] = cursor
            resp = await self.request("GET", "/v1/fleet/vehicles", params=params)
            body = resp.json()
            for v in body.get("data", []):
                yield v
            pagination = body.get("pagination") or {}
            if not pagination.get("hasNextPage"):
                break
            cursor = pagination.get("endCursor")
            if not cursor:
                break

    async def get_vehicle_locations(
        self, *, start_time: str, end_time: str
    ) -> list[dict[str, Any]]:
        resp = await self.request(
            "GET",
            "/v1/fleet/vehicles/locations/history",
            params={"startTime": start_time, "endTime": end_time},
        )
        return resp.json().get("data", [])

    async def send_driver_message(self, driver_id: str, text: str) -> dict[str, Any]:
        resp = await self.request(
            "POST",
            "/v1/fleet/drivers/messages",
            json={"driverIds": [driver_id], "text": text},
        )
        return resp.json()

    async def list_addresses(self) -> list[dict[str, Any]]:
        """Samsara's term for geofences/named locations."""
        resp = await self.request("GET", "/v1/addresses")
        return resp.json().get("data", [])
