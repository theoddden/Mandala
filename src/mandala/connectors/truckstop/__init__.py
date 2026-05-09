"""Truckstop — load-board posting connector (outbound only).

Used by :mod:`mandala.loadboard` for auto-posting available capacity.
Requires Truckstop partner credentials (integration_id + username +
password). Without them, ``is_configured() == False`` and the
load-board poster gracefully skips it.

Reference: Truckstop Partner API — REST endpoint
``POST {base_url}/v1/postings/trucks``.
"""
from mandala.connectors.truckstop.client import TruckstopClient
from mandala.connectors.truckstop.connector import TruckstopConnector

__all__ = ["TruckstopClient", "TruckstopConnector"]
