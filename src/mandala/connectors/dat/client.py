"""Async client for the DAT One load-board API.

Implements the documented OAuth2 client-credentials flow plus a single
``post_truck`` operation. The ``access_token`` is cached for its full
TTL minus a 60s safety margin.

The DAT API expects equipment codes from its own taxonomy (e.g. ``V`` for
van, ``R`` for reefer); :data:`EQUIPMENT_TO_DAT_CODE` translates from the
canonical Mandala :class:`EquipmentType`.
"""
from __future__ import annotations

import time
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


# DAT equipment-code mapping. See DAT API documentation for the
# authoritative list; this covers v0.1 supported types.
EQUIPMENT_TO_DAT_CODE: dict[EquipmentType, str] = {
    EquipmentType.VAN: "V",
    EquipmentType.REEFER: "R",
    EquipmentType.FLATBED: "F",
    EquipmentType.STEPDECK: "SD",
    EquipmentType.DOUBLE_DROP: "DD",
    EquipmentType.LOWBOY: "LB",
    EquipmentType.POWER_ONLY: "PO",
    EquipmentType.CONTAINER: "C",
    EquipmentType.HOTSHOT: "HS",
    EquipmentType.AUTO_CARRIER: "AC",
    EquipmentType.TANKER: "T",
    EquipmentType.BOX_TRUCK: "ST",
    EquipmentType.OTHER: "V",
}


class DATAuthError(RuntimeError):
    pass


class DATClient:
    """DAT One client. Use as an async context manager."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        *,
        base_url: str | None = None,
        postings_base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        s = get_settings()
        self._client_id = client_id or s.dat_client_id
        self._client_secret = client_secret or s.dat_client_secret
        self._base = (base_url or s.dat_base_url).rstrip("/")
        self._postings_base = (postings_base_url or s.dat_postings_base_url).rstrip("/")
        self._http = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "mandala/0.1 (+https://github.com/mandala-bridge/mandala)",
            },
        )
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    async def __aenter__(self) -> "DATClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    # --- Auth -------------------------------------------------------------

    async def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        if not self._client_id or not self._client_secret:
            raise DATAuthError("DAT client_id/client_secret not configured")

        resp = await self._http.post(
            f"{self._base}/access/v1/token/organization",
            json={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = body["accessToken"]
        # DAT returns expiresIn in seconds
        self._token_expires_at = time.time() + int(body.get("expiresIn", 3600))
        return self._token

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                token = await self._get_token()
                headers = {**kwargs.pop("headers", {}), "Authorization": f"Bearer {token}"}
                resp = await self._http.request(method, url, headers=headers, **kwargs)
                if resp.status_code == 401:
                    # token expired between cache check and request — force refresh.
                    self._token = None
                    raise httpx.HTTPStatusError(
                        "DAT 401 — re-authing", request=resp.request, response=resp
                    )
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"DAT transient {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    # --- Postings ---------------------------------------------------------

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
        """Create a truck posting on DAT One.

        Returns the JSON response from DAT (the posting record including
        DAT's own posting id, which we save to state for later expiry).
        """
        available = available_at or datetime.now(UTC)
        body: dict[str, Any] = {
            "equipmentType": EQUIPMENT_TO_DAT_CODE.get(equipment, "V"),
            "availability": {
                "earliestAvailability": available.isoformat(),
                "latestAvailability": (available + timedelta(hours=ttl_hours)).isoformat(),
            },
            "origin": {
                "type": "GEOLOCATION",
                "latitude": origin_lat,
                "longitude": origin_lon,
            },
            "originDeadheadMiles": radius_mi,
        }
        if origin_city or origin_state:
            body["origin"] = {
                "type": "PLACE",
                "city": origin_city,
                "stateOrProvince": origin_state,
            }
        if destination_states:
            body["destination"] = {
                "type": "STATES",
                "states": destination_states,
            }
        if comments:
            body["comments"] = comments[:140]
        if external_reference:
            body["externalReferenceId"] = external_reference

        resp = await self._request(
            "POST",
            f"{self._postings_base}/postings/v3/truck-postings",
            json=body,
        )
        return resp.json()

    async def expire_truck(self, posting_id: str) -> None:
        """Remove an existing posting by id."""
        await self._request(
            "DELETE",
            f"{self._postings_base}/postings/v3/truck-postings/{posting_id}",
        )
