"""Change tracking detector for audit compliance.

Tracks state changes by comparing current event data with prior state.
Emits audit events when significant field changes are detected.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)


class ChangeTracker:
    """Track state changes for audit trail.

    Compares current event data with prior state from Redis and
    emits audit events when fields change.
    """

    def __init__(self, enabled: bool = True, tracked_fields: list[str] | None = None) -> None:
        """Initialize change tracker.

        Args:
            enabled: Whether tracking is active
            tracked_fields: List of field paths to track (e.g., ["logistics.truck.id", "logistics.location.latlon.latitude"])
                          If None, tracks all fields
        """
        self._enabled = enabled
        self._tracked_fields = tracked_fields

    async def track(self, event: MandalaEvent, prior_state: dict[str, Any] | None) -> dict[str, Any] | None:
        """Compare event with prior state and detect changes.

        Args:
            event: The current event
            prior_state: Prior state from Redis (or None if no prior state)

        Returns:
            Dict of changed fields with old/new values, or None if no changes
        """
        if not self._enabled or not prior_state:
            return None

        current_data = event.data or {}
        changes = {}

        def get_nested_value(obj: dict[str, Any], path: str) -> Any:
            """Get value from nested dict using dot notation."""
            keys = path.split(".")
            value = obj
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            return value

        # Compare tracked fields
        fields_to_check = self._tracked_fields if self._tracked_fields else self._get_all_fields(current_data)

        for field in fields_to_check:
            old_value = get_nested_value(prior_state, field)
            new_value = get_nested_value(current_data, field)

            # Normalize to JSON for comparison
            old_json = json.dumps(old_value, sort_keys=True, default=str) if old_value is not None else None
            new_json = json.dumps(new_value, sort_keys=True, default=str) if new_value is not None else None

            if old_json != new_json:
                changes[field] = {"old": old_value, "new": new_value}

        if changes:
            log.info(
                "change.detected",
                event_id=event.id,
                event_type=event.type,
                subject=event.subject,
                changed_fields=list(changes.keys()),
            )
            return changes

        return None

    def _get_all_fields(self, obj: dict[str, Any], prefix: str = "") -> list[str]:
        """Recursively get all field paths from a dict."""
        fields = []
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            fields.append(path)
            if isinstance(value, dict):
                fields.extend(self._get_all_fields(value, path))
        return fields

    async def __call__(self, event: MandalaEvent, state: object, redis: object) -> list[MandalaEvent]:
        """Detector protocol: compare event data against prior state, emit audit event if changed."""
        if not self._enabled or not event.subject:
            return []
        parts = event.subject.split(":")
        kind = parts[2] if len(parts) >= 4 else "entity"
        prior = await state.get(kind, event.subject)  # type: ignore[attr-defined]
        changes = await self.track(event, prior)
        if changes:
            return [self.create_change_alert_event(event, changes)]
        return []

    def create_change_alert_event(self, event: MandalaEvent, changes: dict[str, Any]) -> MandalaEvent:
        """Create an audit event for state changes.

        Args:
            event: The event that triggered the change
            changes: Dict of changed fields with old/new values

        Returns:
            Audit event
        """
        from mandala.core.events.envelope import new_event

        return new_event(
            type="mandala.audit.state.changed",
            source="mandala/compliance/change_tracker",
            subject=event.subject or "unknown",
            data={
                "original_event_id": event.id,
                "original_event_type": event.type,
                "changes": changes,
                "changed_at": datetime.now(UTC).isoformat(),
            },
        )
