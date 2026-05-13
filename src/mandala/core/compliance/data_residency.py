"""Data residency middleware for GDPR/CCPA compliance.

Rejects events from disallowed geographic regions based on location
attributes in the event. Configured via allowed ISO 3166-1 alpha-2
country codes.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import Request, Response, status

log = structlog.get_logger(__name__)

# Rust acceleration for data residency checks
try:
    from mandala_rust_ext import data_residency_extract_country as rust_data_residency_extract_country
    from mandala_rust_ext import data_residency_check as rust_data_residency_check

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False


class DataResidencyMiddleware:
    """Middleware for data residency compliance.

    Checks event location against allowed regions and rejects
    events from disallowed countries.
    """

    def __init__(self, allowed_regions: list[str] | None = None) -> None:
        """Initialize data residency middleware.

        Args:
            allowed_regions: List of ISO 3166-1 alpha-2 country codes (e.g., ["US", "CA", "MX"])
        """
        self._allowed_regions = set(allowed_regions or [])
        self._enabled = len(self._allowed_regions) > 0

    async def __call__(self, request: Request, call_next) -> Response:
        """Process request and check data residency.

        Args:
            request: The incoming request
            call_next: The next middleware/route handler

        Returns:
            Response or 403 if residency check fails
        """
        if not self._enabled:
            return await call_next(request)

        # Only check POST requests with JSON bodies
        if request.method != "POST":
            return await call_next(request)

        try:
            body = await request.json()
        except Exception:
            # If we can't parse JSON, let it through (will fail validation later)
            return await call_next(request)

        # Extract country from event attributes or data
        country = self._extract_country(body)

        # Use Rust for residency check if available (non-blocking, preserves async architecture)
        if _RUST_EXT_AVAILABLE:
            allowed = rust_data_residency_check(country, list(self._allowed_regions))
            if not allowed:
                log.warning(
                    "data_residency.rejected",
                    country=country,
                    allowed_regions=list(self._allowed_regions),
                    path=request.url.path,
                )
                return Response(
                    content=f"Event from country '{country}' not allowed by data residency policy",
                    status_code=status.HTTP_403_FORBIDDEN,
                )
        else:
            # Fallback to Python logic
            if country and country not in self._allowed_regions:
                log.warning(
                    "data_residency.rejected",
                    country=country,
                    allowed_regions=list(self._allowed_regions),
                    path=request.url.path,
                )
                return Response(
                    content=f"Event from country '{country}' not allowed by data residency policy",
                    status_code=status.HTTP_403_FORBIDDEN,
                )

        return await call_next(request)

    def _extract_country(self, event_body: dict[str, Any]) -> str | None:
        """Extract country code from event.

        Checks common locations:
        - attributes.logistics.location.country
        - data.country
        - data.location.country
        - data.address.country

        Args:
            event_body: The event JSON body

        Returns:
            ISO 3166-1 alpha-2 country code or None
        """
        # Use Rust for country extraction if available (non-blocking, preserves async architecture)
        if _RUST_EXT_AVAILABLE:
            event_json = __import__("json").dumps(event_body)
            return rust_data_residency_extract_country(event_json)

        # Fallback to Python logic
        # Check attributes
        attributes = event_body.get("attributes", {})
        if isinstance(attributes, dict):
            country = attributes.get("logistics.location.country")
            if country:
                return str(country).upper()[:2]  # Normalize to 2-char code

        # Check data
        data = event_body.get("data", {})
        if isinstance(data, dict):
            # Direct country field
            if "country" in data:
                return str(data["country"]).upper()[:2]

            # Nested in location
            location = data.get("location", {})
            if isinstance(location, dict) and "country" in location:
                return str(location["country"]).upper()[:2]

            # Nested in address
            address = data.get("address", {})
            if isinstance(address, dict) and "country" in address:
                return str(address["country"]).upper()[:2]

        return None
