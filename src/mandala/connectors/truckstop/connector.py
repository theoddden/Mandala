"""Truckstop connector glue."""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class TruckstopConnector(BaseConnector):
    slug = "truckstop"
    name = "Truckstop"
    vendor = "Truckstop.com"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.truckstop_integration_id) and bool(s.truckstop_username) and bool(s.truckstop_password)
