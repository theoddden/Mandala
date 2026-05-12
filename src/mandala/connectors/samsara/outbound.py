"""Samsara outbound integration — push enrichment back to Samsara.

Uses Samsara REST API to:
- Update vehicle tags with customs status
- Create driver messages for border crossings
- Update shipment status

This enables Mandala to enrich Samsara dashboard data without requiring
a separate Mandala dashboard.

Reference: https://developers.samsara.com/reference
"""
from __future__ import annotations

import httpx
import structlog

from mandala.core.circuit_breaker import CircuitBreaker
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class SamsaraOutboundClient:
    """Push Mandala enrichment back to Samsara."""

    def __init__(self) -> None:
        s = get_settings()
        self._client = httpx.AsyncClient(
            base_url=s.samsara_base_url,
            headers={"Authorization": f"Bearer {s.samsara_api_token}"},
            timeout=30.0,
        )
        # Circuit breaker for outbound calls to prevent cascading failures
        self._circuit_breaker = CircuitBreaker(
            name="samsara_outbound",
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=httpx.HTTPError,
        )

    async def update_vehicle_tag(
        self,
        vehicle_id: str,
        tag_key: str,
        tag_value: str,
    ) -> None:
        """Update a Samsara vehicle tag (replaces custom fields API).

        Args:
            vehicle_id: Samsara vehicle ID
            tag_key: Tag key
            tag_value: Tag value
        """
        async with self._circuit_breaker:
            try:
                await self._client.patch(
                    f"/v1/fleet/vehicles/{vehicle_id}/tags",
                    json={"tags": {tag_key: tag_value}},
                )
                log.info(
                    "samsara vehicle tag updated",
                    vehicle_id=vehicle_id,
                    tag_key=tag_key,
                    tag_value=tag_value,
                )
            except httpx.HTTPError as exc:
                log.error(
                    "failed to update samsara vehicle tag",
                    vehicle_id=vehicle_id,
                    tag_key=tag_key,
                    error=str(exc),
                )
                raise

    async def send_driver_message(
        self,
        driver_id: str,
        message: str,
    ) -> None:
        """Send a message to a driver (replaces alerts API).

        Args:
            driver_id: Samsara driver ID
            message: Message to send
        """
        async with self._circuit_breaker:
            try:
                await self._client.post(
                    "/v1/fleet/drivers/messages",
                    json={
                        "driverIds": [driver_id],
                        "text": message,
                    },
                )
                log.info(
                    "samsara driver message sent",
                    driver_id=driver_id,
                    message=message,
                )
            except httpx.HTTPError as exc:
                log.error(
                    "failed to send samsara driver message",
                    driver_id=driver_id,
                    error=str(exc),
                )
                raise

    async def close(self) -> None:
        await self._client.aclose()
