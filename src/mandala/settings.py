"""Runtime configuration. All env vars are namespaced ``MANDALA_*``."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MANDALA_", env_file=".env", extra="ignore")

    # Infra
    redis_url: str = "redis://localhost:6379/0"

    # Webhook HMAC secrets.
    # Defaults are intentionally empty so production deployments that forget
    # to set MANDALA_*_WEBHOOK_SECRET fail closed (verify_hmac_sha256 returns
    # False on empty secret) instead of accepting payloads signed with a
    # well-known development value.
    samsara_webhook_secret: str = ""
    descartes_webhook_secret: str = ""

    # Anti-replay window for webhook timestamps. Set to 0 to disable
    # timestamp checking (NOT recommended in production — only useful for
    # vendors that don't expose a timestamp header).
    webhook_timestamp_tolerance_sec: int = 300

    # Outbound API credentials (optional)
    samsara_api_token: str = ""
    samsara_base_url: str = "https://api.samsara.com"
    samsara_outbound_enabled: bool = False  # Push enrichment back to Samsara
    descartes_api_key: str = ""
    descartes_macropoint_base_url: str = "https://api.macropoint.com"

    # CargoWise (WiseTech eAdaptor)
    cargowise_webhook_secret: str = ""
    cargowise_eadaptor_url: str = ""
    cargowise_username: str = ""
    cargowise_password: str = ""
    cargowise_organization_code: str = ""

    # Load-board outbound (DAT One)
    dat_client_id: str = ""
    dat_client_secret: str = ""
    dat_base_url: str = "https://identity.api.dat.com"
    dat_postings_base_url: str = "https://freight.api.dat.com"

    # Load-board behaviour
    loadboard_enabled: bool = False
    loadboard_post_default_radius_mi: int = 250
    loadboard_post_ttl_hours: int = 24

    # Rail (Vizion API)
    vizion_api_key: str = ""

    # Aurora (autonomous trucks) - partnership required
    # See docs/integrations/aurora.md for integration pattern
    aurora_enabled: bool = False  # Disabled by default until partnership available
    aurora_webhook_secret: str = ""
    aurora_api_key: str = ""
    aurora_beacon_enabled: bool = False  # Aurora Beacon platform
    aurora_intelligence_sharing: bool = True  # Share Aurora data with Samsara trucks

    # Streams
    stream_inbound: str = "mandala:events"
    consumer_group: str = "mandala"

    # Alert routing
    alert_routing_enabled: bool = False
    alert_slack_webhook_url: str = ""
    alert_smtp_enabled: bool = False
    alert_smtp_host: str = "smtp.gmail.com"
    alert_smtp_port: int = 587
    alert_smtp_use_tls: bool = True
    alert_smtp_user: str = ""
    alert_smtp_password: str = ""
    alert_smtp_from: str = "mandala@yourdomain.com"
    alert_smtp_to: str = "ops@yourdomain.com"
    alert_pagerduty_routing_key: str = ""

    # Alert aggregation
    alert_aggregation_enabled: bool = False
    alert_aggregation_window_seconds: int = 300  # 5 minutes

    # Alert suppression
    alert_suppression_enabled: bool = False
    # [{"start": "2024-01-01T00:00:00Z", "end": "2024-01-01T06:00:00Z"}]
    alert_suppression_windows: list[dict[str, str]] = Field(default_factory=list)

    # Materialized views (CQRS read models — run via ``mandala views``)
    views_enabled: bool = False
    views_consumer_group: str = "mandala:views"
    views_geospatial_enabled: bool = True
    views_timeseries_enabled: bool = True
    views_bitmap_enabled: bool = True
    views_graph_enabled: bool = False  # requires RedisGraph/FalkorDB module
    views_timeseries_ttl_seconds: int = 7 * 86_400  # 7 days of cold-chain readings

    # Metrics (Prometheus)
    metrics_enabled: bool = False
    metrics_port: int = 9090

    # Misc
    log_level: str = "INFO"
    state_ttl_seconds: int = 14 * 86_400


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
