"""PII detection detector for compliance.

Scans event data for common PII patterns (emails, SSNs, phone numbers, etc.)
and emits alert events if PII is detected. Runs in detector sandbox with
timeout and circuit breaker protection.
"""

from __future__ import annotations

import re
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)


# Common PII patterns (lightweight regex-based detection)
PII_PATTERNS = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "ssn": r"\b\d{3}-\d{2}-\d{4}\b",  # US SSN format
    "phone_us": r"\b\d{3}-\d{3}-\d{4}\b",  # US phone format
    "phone_intl": r"\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
    "credit_card": r"\b(?:\d[ -]*?){13,16}\b",  # Credit card numbers (loose)
    "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
}


class PIIDetector:
    """Detect PII in event data.

    Lightweight regex-based scanning for common PII patterns.
    Emits alert events when PII is detected.
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize PII detector.

        Args:
            enabled: Whether detection is active
        """
        self._enabled = enabled
        self._compiled_patterns = {name: re.compile(pattern) for name, pattern in PII_PATTERNS.items()}

    async def detect(self, event: MandalaEvent) -> dict[str, Any] | None:
        """Scan event for PII patterns.

        Args:
            event: The MandalaEvent to scan

        Returns:
            Dict with detected PII types and field names, or None if no PII found
        """
        if not self._enabled:
            return None

        detected = {}
        event_dict = event.model_dump(exclude_none=True)

        # Recursively scan all string values in the event
        def scan_dict(obj: Any, path: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    scan_dict(value, f"{path}.{key}" if path else key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    scan_dict(item, f"{path}[{i}]")
            elif isinstance(obj, str):
                for pii_type, pattern in self._compiled_patterns.items():
                    if pattern.search(obj):
                        if pii_type not in detected:
                            detected[pii_type] = []
                        detected[pii_type].append(path)

        scan_dict(event_dict)

        if detected:
            log.info(
                "pii.detected",
                event_id=event.id,
                event_type=event.type,
                pii_types=list(detected.keys()),
            )
            return detected

        return None

    def create_pii_alert_event(self, event: MandalaEvent, pii_detected: dict[str, Any]) -> MandalaEvent:
        """Create an alert event for PII detection.

        Args:
            event: The original event containing PII
            pii_detected: Dict of detected PII types and locations

        Returns:
            Alert event
        """
        from mandala.core.events.envelope import new_event

        return new_event(
            type="mandala.privacy.pii.detected",
            source="mandala/compliance/pii_detector",
            subject=event.subject or "unknown",
            data={
                "original_event_id": event.id,
                "original_event_type": event.type,
                "pii_detected": pii_detected,
            },
        )
