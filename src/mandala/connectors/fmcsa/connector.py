"""FMCSA connector glue.

FMCSA SAFER API is a free, public API with no authentication required.
This connector is always "configured" since no credentials are needed.
"""

from __future__ import annotations

from mandala.connectors.base import BaseConnector


class FMCSAConnector(BaseConnector):
    slug = "fmcsa"
    name = "FMCSA SAFER"
    vendor = "Federal Motor Carrier Safety Administration"

    def is_configured(self) -> bool:
        """FMCSA SAFER API is free and public — no credentials required."""
        return True
