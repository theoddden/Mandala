"""CargoWise connector glue."""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class CargoWiseConnector(BaseConnector):
    slug = "cargowise"
    name = "WiseTech CargoWise"
    vendor = "WiseTech Global"

    def is_configured(self) -> bool:
        s = get_settings()
        # Webhook flow only needs the secret. Outbound client also needs
        # eAdaptor URL + Basic-auth credentials.
        return bool(s.cargowise_webhook_secret) or bool(
            s.cargowise_eadaptor_url
            and s.cargowise_username
            and s.cargowise_password
        )
