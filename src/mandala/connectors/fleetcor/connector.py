"""Top-level FLEETCOR connector glue."""

from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class FleetcorConnector(BaseConnector):
    slug = "fleetcor"
    name = "FLEETCOR"
    vendor = "FLEETCOR (Comdata, Fuelman, FleetONE)"

    def is_configured(self) -> bool:
        s = get_settings()
        return bool(s.fleetcor_api_key)
