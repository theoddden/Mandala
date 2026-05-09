"""Top-level Samsara connector glue."""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class SamsaraConnector(BaseConnector):
    slug = "samsara"
    name = "Samsara"
    vendor = "Samsara Connected Operations Platform"

    def is_configured(self) -> bool:
        s = get_settings()
        # The webhook flow only needs a secret. The polling/outbound flow
        # also needs an API token, but having only one is still useful.
        return bool(s.samsara_webhook_secret) or bool(s.samsara_api_token)
