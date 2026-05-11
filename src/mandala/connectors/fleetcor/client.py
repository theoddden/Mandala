"""FLEETCOR API client for fuel transaction polling.

FLEETCOR provides fuel card management with a REST API for transaction data.
Supports multiple brands: Comdata, Fuelman, FleetONE, etc.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import aiohttp


class FleetcorClient:
    """Async client for FLEETCOR fuel card API."""

    def __init__(self, api_key: str, account_id: str, base_url: str = "https://api.fleetcor.com/v1"):
        self.api_key = api_key
        self.account_id = account_id
        self.base_url = base_url
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "X-Account-ID": self.account_id,
            }
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        truck_id: str | None = None,
        card_number: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch fuel transactions from FLEETCOR API.

        Args:
            start_date: Start of date range for transactions
            end_date: End of date range for transactions
            truck_id: Optional filter for specific truck
            card_number: Optional filter for specific card
            limit: Maximum number of transactions to return

        Returns:
            List of transaction dictionaries from FLEETCOR API
        """
        session = await self._get_session()
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "limit": limit,
        }
        if truck_id:
            params["vehicleId"] = truck_id
        if card_number:
            params["cardNumber"] = card_number

        async with session.get(
            f"{self.base_url}/transactions",
            params=params,
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("transactions", [])

    async def get_vehicles(self) -> list[dict[str, Any]]:
        """Fetch all vehicles associated with the FLEETCOR account.

        Returns:
            List of vehicle dictionaries
        """
        session = await self._get_session()
        async with session.get(f"{self.base_url}/vehicles") as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("vehicles", [])
