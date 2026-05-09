"""Runtime configuration. All env vars are namespaced ``MANDALA_*``."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MANDALA_", env_file=".env", extra="ignore")

    # Infra
    redis_url: str = "redis://localhost:6379/0"

    # Webhook HMAC secrets
    samsara_webhook_secret: str = "dev-secret"
    descartes_webhook_secret: str = "dev-secret"

    # Outbound API credentials (optional)
    samsara_api_token: str = ""
    samsara_base_url: str = "https://api.samsara.com"
    samsara_outbound_enabled: bool = False  # Push enrichment back to Samsara
    descartes_api_key: str = ""
    descartes_macropoint_base_url: str = "https://api.macropoint.com"

    # CargoWise (WiseTech eAdaptor)
    cargowise_webhook_secret: str = "dev-secret"
    cargowise_eadaptor_url: str = ""
    cargowise_username: str = ""
    cargowise_password: str = ""
    cargowise_organization_code: str = ""

    # Load-board outbound (DAT One)
    dat_client_id: str = ""
    dat_client_secret: str = ""
    dat_base_url: str = "https://identity.api.dat.com"
    dat_postings_base_url: str = "https://freight.api.dat.com"

    # Load-board outbound (Truckstop)
    truckstop_integration_id: str = ""
    truckstop_username: str = ""
    truckstop_password: str = ""
    truckstop_base_url: str = "https://api.truckstop.com"

    # Load-board behaviour
    loadboard_enabled: bool = False
    loadboard_post_default_radius_mi: int = 250
    loadboard_post_ttl_hours: int = 24

    # Rail (Vizion API)
    vizion_api_key: str = ""

    # Streams
    stream_inbound: str = "mandala:events"
    consumer_group: str = "mandala"

    # Misc
    log_level: str = "INFO"
    state_ttl_seconds: int = 14 * 86_400


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
