"""Coast API client for fuel transaction polling.

Coast provides a REST API for querying fuel transactions.
Reference: Coast API documentation (available via Samsara Marketplace)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import aiohttp


class CoastClient:
    """Async client for Coast fuel card API."""

    def __init__(self, api_key: str, base_url: str = "https://api.coastpay.com/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {"Authorization": f"Bearer {self.api_key}"}
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
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch fuel transactions from Coast API.

        Args:
            start_date: Start of date range for transactions
            end_date: End of date range for transactions
            truck_id: Optional filter for specific truck
            limit: Maximum number of transactions to return

        Returns:
            List of transaction dictionaries from Coast API
        """
        session = await self._get_session()
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "limit": limit,
        }
        if truck_id:
            params["truckId"] = truck_id

        async with session.get(
            f"{self.base_url}/transactions",
            params=params,
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("transactions", [])

    async def get_trucks(self) -> list[dict[str, Any]]:
        """Fetch all trucks associated with the Coast account.

        Returns:
            List of truck dictionaries
        """
        session = await self._get_session()
        async with session.get(f"{self.base_url}/trucks") as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("trucks", [])
