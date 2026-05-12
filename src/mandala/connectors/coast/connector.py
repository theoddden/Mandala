"""Top-level Coast connector glue."""

from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class CoastConnector(BaseConnector):
    slug = "coast"
    name = "Coast"
    vendor = "Coast Fuel Card"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.coast_api_key)
