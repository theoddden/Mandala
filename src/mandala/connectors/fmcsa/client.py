"""Async client for the FMCSA SAFER API.

The SAFER (Safety and Fitness Electronic Records) API is a free, public API
provided by the FMCSA with no authentication required. It provides carrier
safety data including CSA scores, inspection history, violation records,
out-of-service rate, and operating authority status.

Reference: https://developer.fmcsa.dot.gov/api-reference
"""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class FMCSAClient:
    """FMCSA SAFER API client. Use as an async context manager."""

    BASE_URL = "https://mobile.fmcsa.dot.gov/qc/services/carriers"

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base = (base_url or self.BASE_URL).rstrip("/")
        self._http = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "mandala/0.1 (+https://github.com/theoddden/Mandala)",
            },
        )

    async def __aenter__(self) -> FMCSAClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._http.request(method, url, **kwargs)
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"FMCSA transient {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    async def get_carrier_by_dot(self, dot_number: str) -> dict[str, Any]:
        """Fetch carrier safety profile by DOT number.

        Returns a dict containing:
        - Basic carrier info (name, address, operating authority)
        - CSA scores across all seven BASIC categories
        - Inspection history (24-month summary)
        - Violation records
        - Out-of-service rate

        Args:
            dot_number: The FMCSA DOT number (string, e.g. "1234567")

        Returns:
            Dict with carrier safety profile data.

        Raises:
            httpx.HTTPStatusError: If the DOT number is invalid or API error
        """
        # SAFER API uses the carrier endpoint with DOT number parameter
        url = f"{self._base}/{dot_number}"
        resp = await self._request("GET", url)
        data = resp.json()

        # The SAFER API returns a nested structure; we normalize it
        content = data.get("content", {})

        # Extract CSA scores (BASIC categories)
        csa_scores = content.get("carrier", {}).get("safety_rating", {})

        # Extract inspection summary
        inspections = content.get("carrier", {}).get("inspections", {})

        # Extract operating authority
        authority = content.get("carrier", {}).get("authority", {})

        return {
            "dot_number": dot_number,
            "carrier_name": content.get("carrier", {}).get("carrier_name"),
            "legal_name": content.get("carrier", {}).get("legal_name"),
            "dba_name": content.get("carrier", {}).get("dba_name"),
            "address": content.get("carrier", {}).get("address"),
            "city": content.get("carrier", {}).get("city"),
            "state": content.get("carrier", {}).get("state"),
            "zip": content.get("carrier", {}).get("zip"),
            "phone": content.get("carrier", {}).get("phone"),
            "email": content.get("carrier", {}).get("email"),
            "operating_status": authority.get("operating_status"),
            "authority_type": authority.get("authority_type"),
            "authority_number": authority.get("authority_number"),
            "authority_expiration": authority.get("expiration_date"),
            # CSA Scores (BASIC categories)
            "csa_scores": {
                "unsafe_driving": csa_scores.get("unsafe_driving"),
                "crash_indicator": csa_scores.get("crash_indicator"),
                "hours_of_service_compliance": csa_scores.get("hos_compliance"),
                "vehicle_maintenance": csa_scores.get("vehicle_maintenance"),
                "controlled_substances_alcohol": csa_scores.get("controlled_substances"),
                "driver_fitness": csa_scores.get("driver_fitness"),
                "hazardous_materials": csa_scores.get("hazardous_materials"),
            },
            # Inspection summary (24-month)
            "inspections_24mo": {
                "vehicle_inspections": inspections.get("vehicle_count", 0),
                "driver_inspections": inspections.get("driver_count", 0),
                "hazmat_inspections": inspections.get("hazmat_count", 0),
                "out_of_service_rate": inspections.get("out_of_service_rate", 0.0),
            },
            # Safety rating
            "safety_rating": content.get("carrier", {}).get("safety_rating", {}).get("rating"),
            "safety_rating_date": content.get("carrier", {}).get("safety_rating", {}).get("rating_date"),
            # API metadata
            "last_updated": content.get("last_updated"),
        }
