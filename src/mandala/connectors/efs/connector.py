"""Top-level EFS connector glue."""

from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class EfsConnector(BaseConnector):
    slug = "efs"
    name = "EFS"
    vendor = "EFS (Electronic Fuel Systems)"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.efs_api_key)
