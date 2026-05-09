"""Samsara outbound integration — push enrichment back to Samsara.

Uses Samsara REST API to:
- Update custom fields with customs status
- Create alerts for border crossings
- Update shipment status
- Push carrier safety scores

This enables Mandala to enrich Samsara dashboard data without requiring
a separate Mandala dashboard.
"""
from __future__ import annotations

import httpx
from datetime import UTC, datetime

import structlog

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

    async def update_custom_field(
        self,
        vehicle_id: str,
        field_id: str,
        value: str,
    ) -> None:
        """Update a Samsara custom field.

        Args:
            vehicle_id: Samsara vehicle ID
            field_id: Samsara custom field ID
            value: Field value
        """
        try:
            await self._client.patch(
                f"/v1/fleet/vehicles/{vehicle_id}/customFields",
                json={field_id: value},
            )
            log.info(
                "samsara custom field updated",
                vehicle_id=vehicle_id,
                field_id=field_id,
                value=value,
            )
        except httpx.HTTPError as exc:
            log.error(
                "failed to update samsara custom field",
                vehicle_id=vehicle_id,
                field_id=field_id,
                error=str(exc),
            )

    async def create_alert(
        self,
        vehicle_id: str,
        alert_type: str,
        severity: str,
        message: str,
    ) -> None:
        """Create an alert in Samsara.

        Args:
            vehicle_id: Samsara vehicle ID
            alert_type: Alert type identifier
            severity: Alert severity (INFO, WARNING, CRITICAL)
            message: Alert message
        """
        try:
            await self._client.post(
                "/v1/alerts",
                json={
                    "vehicleId": vehicle_id,
                    "type": alert_type,
                    "severity": severity,
                    "message": message,
                    "occurredAt": datetime.now(UTC).isoformat(),
                },
            )
            log.info(
                "samsara alert created",
                vehicle_id=vehicle_id,
                alert_type=alert_type,
                severity=severity,
            )
        except httpx.HTTPError as exc:
            log.error(
                "failed to create samsara alert",
                vehicle_id=vehicle_id,
                alert_type=alert_type,
                error=str(exc),
            )

    async def close(self) -> None:
        await self._client.aclose()
