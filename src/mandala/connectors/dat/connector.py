"""DAT connector glue."""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class DATConnector(BaseConnector):
    slug = "dat"
    name = "DAT One"
    vendor = "DAT Freight & Analytics"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.dat_client_id) and bool(s.dat_client_secret)
