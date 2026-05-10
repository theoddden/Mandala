"""Alert routing to external notification channels.

Routes Mandala alerts to Slack, Email, PagerDuty, and other notification
channels based on alert type, severity, and routing rules.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class AlertRouter:
    """Routes alerts to external notification channels."""

    def __init__(self) -> None:
        self._http_client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def close(self) -> None:
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def route(self, event: MandalaEvent) -> None:
        """Route an alert event to configured notification channels.

        Args:
            event: Mandala alert event to route
        """
        s = get_settings()
        if not s.alert_routing_enabled:
            return

        data = event.data if isinstance(event.data, dict) else {}
        alert_type = event.type
        severity = data.get("severity", "unknown")

        # Route to Slack if configured
        if s.alert_slack_webhook_url:
            await self._route_to_slack(event, data, severity)

        # Route to Email if configured
        if s.alert_smtp_enabled:
            await self._route_to_email(event, data, severity)

        # Route to PagerDuty if configured
        if s.alert_pagerduty_routing_key:
            await self._route_to_pagerduty(event, data, severity)

    async def _route_to_slack(
        self, event: MandalaEvent, data: dict[str, Any], severity: str
    ) -> None:
        """Route alert to Slack webhook.

        Args:
            event: Mandala alert event
            data: Event data payload
            severity: Alert severity
        """
        s = get_settings()
        if not s.alert_slack_webhook_url:
            return

        try:
            client = await self._get_client()

            # Map severity to Slack color
            color_map = {
                "critical": "#ff0000",
                "high": "#ff6600",
                "warning": "#ffcc00",
                "info": "#00ccff",
                "low": "#00cc00",
            }
            color = color_map.get(severity, "#808080")

            # Build Slack message
            slack_payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"Mandala Alert: {event.type}",
                        "fields": [
                            {"title": "Severity", "value": severity, "short": True},
                            {
                                "title": "Entity",
                                "value": event.subject or "unknown",
                                "short": True,
                            },
                            {
                                "title": "Reason",
                                "value": data.get("reason", "No reason provided"),
                                "short": False,
                            },
                        ],
                        "footer": "Mandala",
                        "ts": int(event.time.timestamp()) if event.time else None,
                    }
                ]
            }

            # Add optional fields if present
            if data.get("truck_urn"):
                slack_payload["attachments"][0]["fields"].append(
                    {"title": "Truck", "value": data["truck_urn"], "short": True}
                )
            if data.get("shipment_urn"):
                slack_payload["attachments"][0]["fields"].append(
                    {"title": "Shipment", "value": data["shipment_urn"], "short": True}
                )
            if data.get("border_poe"):
                slack_payload["attachments"][0]["fields"].append(
                    {"title": "Border POE", "value": data["border_poe"], "short": True}
                )

            response = await client.post(s.alert_slack_webhook_url, json=slack_payload)
            response.raise_for_status()

            log.info(
                "alert.routed.slack",
                alert_type=event.type,
                severity=severity,
                status_code=response.status_code,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("alert.routing.slack_failed", error=str(exc))

    async def _route_to_email(
        self, event: MandalaEvent, data: dict[str, Any], severity: str
    ) -> None:
        """Route alert to Email via SMTP.

        Args:
            event: Mandala alert event
            data: Event data payload
            severity: Alert severity
        """
        s = get_settings()
        if not s.alert_smtp_enabled:
            return

        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # Build email (cheap, sync)
        msg = MIMEMultipart()
        msg["From"] = s.alert_smtp_from
        msg["To"] = s.alert_smtp_to
        msg["Subject"] = f"[Mandala {severity.upper()}] {event.type}"

        body = (
            "Mandala Alert Detected\n\n"
            f"Type: {event.type}\n"
            f"Severity: {severity}\n"
            f"Subject: {event.subject or 'unknown'}\n"
            f"Reason: {data.get('reason', 'No reason provided')}\n"
            f"Time: {event.time.isoformat() if event.time else 'unknown'}\n\n"
        )
        if data.get("truck_urn"):
            body += f"Truck: {data['truck_urn']}\n"
        if data.get("shipment_urn"):
            body += f"Shipment: {data['shipment_urn']}\n"
        if data.get("border_poe"):
            body += f"Border POE: {data['border_poe']}\n"
        msg.attach(MIMEText(body, "plain"))

        def _send_sync() -> None:
            with smtplib.SMTP(s.alert_smtp_host, s.alert_smtp_port, timeout=10) as server:
                if s.alert_smtp_use_tls:
                    server.starttls()
                if s.alert_smtp_user and s.alert_smtp_password:
                    server.login(s.alert_smtp_user, s.alert_smtp_password)
                server.send_message(msg)

        # Offload blocking SMTP I/O so the event loop is not stalled.
        try:
            await asyncio.to_thread(_send_sync)
            log.info(
                "alert.routed.email",
                alert_type=event.type,
                severity=severity,
                recipient=s.alert_smtp_to,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("alert.routing.email_failed", error=str(exc))

    async def _route_to_pagerduty(
        self, event: MandalaEvent, data: dict[str, Any], severity: str
    ) -> None:
        """Route alert to PagerDuty via Events API v2.

        Args:
            event: Mandala alert event
            data: Event data payload
            severity: Alert severity
        """
        s = get_settings()
        if not s.alert_pagerduty_routing_key:
            return

        # Only route critical/high severity to PagerDuty
        if severity not in ("critical", "high"):
            return

        try:
            client = await self._get_client()

            # Map Mandala severity to PagerDuty severity
            severity_map = {
                "critical": "critical",
                "high": "error",
                "warning": "warning",
                "info": "info",
                "low": "info",
            }
            pd_severity = severity_map.get(severity, "info")

            # Build PagerDuty event
            pd_payload = {
                "routing_key": s.alert_pagerduty_routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": f"Mandala Alert: {event.type}",
                    "severity": pd_severity,
                    "source": "mandala",
                    "custom_details": {
                        "alert_type": event.type,
                        "severity": severity,
                        "subject": event.subject or "unknown",
                        "reason": data.get("reason", "No reason provided"),
                        "truck_urn": data.get("truck_urn"),
                        "shipment_urn": data.get("shipment_urn"),
                    },
                },
            }

            response = await client.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=pd_payload,
            )
            response.raise_for_status()

            log.info(
                "alert.routed.pagerduty",
                alert_type=event.type,
                severity=severity,
                dedup_key=response.json().get("deduplication_key"),
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("alert.routing.pagerduty_failed", error=str(exc))
