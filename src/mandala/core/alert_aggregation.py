"""Alert aggregation to prevent alert spam.

Aggregates similar alerts within a time window to prevent alert fatigue.
Groups alerts by type, entity, and severity before routing.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class Alert:
    """Represents a single alert."""

    def __init__(
        self,
        id: str,
        type: str,
        severity: str,
        message: str,
        entity_id: str | None = None,
        timestamp: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.id = id
        self.type = type
        self.severity = severity
        self.message = message
        self.entity_id = entity_id
        self.timestamp = timestamp or datetime.now(UTC)
        self.metadata = metadata or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "severity": self.severity,
            "message": self.message,
            "entity_id": self.entity_id,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


class AlertGroup:
    """Represents a group of aggregated alerts."""

    def __init__(
        self,
        id: str,
        alert_type: str,
        entity_id: str | None = None,
        severity: str = "unknown",
        alerts: list[Alert] | None = None,
    ) -> None:
        self.id = id
        self.alert_type = alert_type
        self.entity_id = entity_id
        self.severity = severity
        self.alerts = alerts or []
        self.created_at = datetime.now(UTC)

    def add_alert(self, alert: Alert) -> None:
        """Add an alert to the group."""
        self.alerts.append(alert)

    def get_count(self) -> int:
        """Get the number of alerts in the group."""
        return len(self.alerts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "alert_type": self.alert_type,
            "entity_id": self.entity_id,
            "severity": self.severity,
            "count": self.get_count(),
            "created_at": self.created_at.isoformat(),
            "alerts": [alert.to_dict() for alert in self.alerts],
        }


class AlertAggregator:
    """Aggregates similar alerts within a time window."""

    def __init__(self, redis: object) -> None:
        self._redis = redis
        self._aggregation_key_prefix = "mandala:alert:aggregation"

    def _aggregation_key(self, event: MandalaEvent) -> str:
        """Generate aggregation key for an alert event.

        Groups alerts by: type + subject (entity) + severity
        """
        data = event.data if isinstance(event.data, dict) else {}
        severity = data.get("severity", "unknown")
        subject = event.subject or "unknown"
        return f"{self._aggregation_key_prefix}:{event.type}:{subject}:{severity}"

    async def should_route(self, event: MandalaEvent) -> bool:
        """Determine if an alert should be routed or aggregated.

        Returns True if the alert should be routed immediately,
        False if it's aggregated into a pending batch.

        Args:
            event: Mandala alert event

        Returns:
            True if should route, False if aggregated
        """
        s = get_settings()
        if not s.alert_aggregation_enabled:
            return True

        # Check if alert suppression is active
        if s.alert_suppression_enabled:
            if await self._is_suppressed(event):
                log.info("alert.suppressed", alert_type=event.type, subject=event.subject)
                return False

        key = self._aggregation_key(event)

        # Check if aggregation window is active
        existing = await self._redis.get(key)  # type: ignore[attr-defined]
        if existing:
            if isinstance(existing, bytes):
                existing = existing.decode()
            agg_data = json.loads(existing)
            agg_data["count"] += 1
            agg_data["last_alert_at"] = datetime.now(UTC).isoformat()
            agg_data["alert_ids"].append(event.id)

            # Update aggregation with new count.
            # redis-py SETEX signature: setex(name, time, value).
            await self._redis.setex(  # type: ignore[attr-defined]
                key,
                s.alert_aggregation_window_seconds,
                json.dumps(agg_data),
            )

            log.debug(
                "alert.aggregated",
                key=key,
                count=agg_data["count"],
                alert_type=event.type,
            )
            return False
        # Start new aggregation window
        agg_data = {
            "count": 1,
            "first_alert_at": datetime.now(UTC).isoformat(),
            "last_alert_at": datetime.now(UTC).isoformat(),
            "alert_ids": [event.id],
            "alert_type": event.type,
            "subject": event.subject,
        }

        # redis-py SETEX signature: setex(name, time, value).
        await self._redis.setex(  # type: ignore[attr-defined]
            key,
            s.alert_aggregation_window_seconds,
            json.dumps(agg_data),
        )

        # Route first alert immediately
        return True

    async def _is_suppressed(self, event: MandalaEvent) -> bool:
        """Check if alert is within a suppression window.

        Args:
            event: Mandala alert event

        Returns:
            True if suppressed, False otherwise
        """
        s = get_settings()
        if not s.alert_suppression_windows:
            return False

        now = datetime.now(UTC)

        for window in s.alert_suppression_windows:
            try:
                start = datetime.fromisoformat(window["start"])
                end = datetime.fromisoformat(window["end"])

                if start <= now <= end:
                    log.info(
                        "alert.suppression_window_active",
                        start=start,
                        end=end,
                        alert_type=event.type,
                    )
                    return True
            except (KeyError, ValueError) as exc:
                log.warning(
                    "alert.suppression_window_invalid",
                    window=window,
                    error=str(exc),
                )

        return False

    async def get_aggregated_alerts(self) -> list[dict[str, Any]]:
        """Get all currently aggregated alerts.

        Returns:
            List of aggregated alert data
        """
        try:
            pattern = f"{self._aggregation_key_prefix}:*"
            keys = []
            cursor = 0

            while True:
                cursor, batch = await self._redis.scan(cursor, match=pattern, count=100)  # type: ignore[attr-defined]
                keys.extend(batch)
                if cursor == 0:
                    break

            aggregated = []
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                raw = await self._redis.get(key)  # type: ignore[attr-defined]
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    data = json.loads(raw)
                    data["key"] = key
                    aggregated.append(data)

            return aggregated
        except Exception as exc:  # noqa: BLE001
            log.exception("alert.aggregation.get_failed", error=str(exc))
            return []

    async def flush_aggregation(self, event: MandalaEvent) -> dict[str, Any] | None:
        """Flush aggregated alerts for this alert type/entity.

        Returns the aggregated data and clears the aggregation window.

        Args:
            event: Mandala alert event

        Returns:
            Aggregated data if found, None otherwise
        """
        key = self._aggregation_key(event)
        raw = await self._redis.get(key)  # type: ignore[attr-defined]

        if raw:
            if isinstance(raw, bytes):
                raw = raw.decode()
            agg_data = json.loads(raw)

            # Delete aggregation key
            await self._redis.delete(key)  # type: ignore[attr-defined]

            log.info(
                "alert.aggregation.flushed",
                key=key,
                count=agg_data["count"],
            )

            return agg_data

        return None
