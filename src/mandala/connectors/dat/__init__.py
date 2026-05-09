"""DAT One — load-board posting connector (outbound only).

Used by :mod:`mandala.loadboard` to auto-post available trucks when a
Samsara delivery confirmation event indicates the asset is empty.

DAT requires partner credentials (client_id + client_secret); without
them the connector returns ``is_configured() == False`` and the
load-board poster gracefully skips it.

Reference: DAT Developer portal — OAuth2 client-credentials flow,
``POST {postings_base_url}/postings/v3/truck-postings``.
"""
from mandala.connectors.dat.client import DATClient
from mandala.connectors.dat.connector import DATConnector

__all__ = ["DATClient", "DATConnector"]
