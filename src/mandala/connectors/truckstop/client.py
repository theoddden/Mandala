"""Async client for the Truckstop Partner API.

Uses HTTP Basic auth (integration_id + username + password concatenated
per Truckstop's spec). Like the DAT client, exposes a single
``post_truck`` operation that the load-board auto-poster invokes.
"""
from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from mandala.core.schema.truck import EquipmentType
from mandala.settings import get_settings


# Truckstop equipment-id mapping. Numeric IDs per their published
# integration spec; verify against your account's accepted set at runtime.
EQUIPMENT_TO_TRUCKSTOP_ID: dict[EquipmentType, int] = {
    EquipmentType.VAN: 1,
    EquipmentType.REEFER: 2,
    EquipmentType.FLATBED: 3,
    EquipmentType.STEPDECK: 4,
    EquipmentType.DOUBLE_DROP: 5,
    EquipmentType.LOWBOY: 6,
    EquipmentType.POWER_ONLY: 7,
    EquipmentType.CONTAINER: 8,
    EquipmentType.HOTSHOT: 9,
    EquipmentType.AUTO_CARRIER: 10,
    EquipmentType.TANKER: 11,
    EquipmentType.BOX_TRUCK: 12,
    EquipmentType.OTHER: 1,
}


class TruckstopClient:
    """Truckstop Partner API client. Use as an async context manager."""

    def __init__(
        self,
        integration_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        s = get_settings()
        self._integration_id = integration_id or s.truckstop_integration_id
        self._username = username or s.truckstop_username
        self._password = password or s.truckstop_password
        self._base = (base_url or s.truckstop_base_url).rstrip("/")
        creds = f"{self._integration_id}:{self._username}:{self._password}".encode()
        self._auth_header = "Basic " + base64.b64encode(creds).decode("ascii")
        self._http = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={
                "Authorization": self._auth_header,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "mandala/0.1 (+https://github.com/mandala-bridge/mandala)",
            },
        )

    async def __aenter__(self) -> "TruckstopClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._http.request(method, path, **kwargs)
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"Truckstop transient {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    async def post_truck(
        self,
        *,
        equipment: EquipmentType,
        origin_lat: float,
        origin_lon: float,
        origin_city: str | None = None,
        origin_state: str | None = None,
        available_at: datetime | None = None,
        radius_mi: int = 250,
        destination_states: list[str] | None = None,
        comments: str | None = None,
        ttl_hours: int = 24,
        external_reference: str | None = None,
    ) -> dict[str, Any]:
        available = available_at or datetime.now(UTC)
        body: dict[str, Any] = {
            "equipmentTypeId": EQUIPMENT_TO_TRUCKSTOP_ID.get(equipment, 1),
            "availableDate": available.isoformat(),
            "expiresAt": (available + timedelta(hours=ttl_hours)).isoformat(),
            "origin": {
                "latitude": origin_lat,
                "longitude": origin_lon,
                "city": origin_city,
                "stateProvince": origin_state,
                "radiusMiles": radius_mi,
            },
            "destinationStates": destination_states or [],
            "comments": (comments or "")[:255],
        }
        if external_reference:
            body["externalReferenceId"] = external_reference

        resp = await self._request("POST", "/v1/postings/trucks", json=body)
        return resp.json()

    async def expire_truck(self, posting_id: str) -> None:
        await self._request("DELETE", f"/v1/postings/trucks/{posting_id}")
