"""MacroPoint connector glue."""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class MacroPointConnector(BaseConnector):
    slug = "descartes-macropoint"
    name = "Descartes MacroPoint"
    vendor = "Descartes Systems Group"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.descartes_api_key) or bool(s.descartes_webhook_secret)
