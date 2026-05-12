"""Top-level WEX connector glue."""

from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class WexConnector(BaseConnector):
    slug = "wex"
    name = "WEX"
    vendor = "WEX Fleet Fuel Cards"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.wex_api_key)
