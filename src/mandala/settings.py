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

    # Fuel cards (Coast, FLEETCOR/Comdata, WEX, EFS)
    coast_api_key: str = ""
    fleetcor_api_key: str = ""
    fleetcor_account_id: str = ""
    wex_api_key: str = ""
    wex_account_id: str = ""
    efs_api_key: str = ""
    efs_account_id: str = ""

    # Aurora (autonomous trucks) - partnership required
    # See docs/integrations/aurora.md for integration pattern
    aurora_enabled: bool = False  # Disabled by default until partnership available
    aurora_webhook_secret: str = ""
    aurora_api_key: str = ""
    aurora_beacon_enabled: bool = False  # Aurora Beacon platform
    aurora_intelligence_sharing: bool = True  # Share Aurora data with Samsara trucks

    # SAP (Transportation Management, Extended Warehouse Management)
    # See docs/integrations/sap.md for integration pattern
    sap_enabled: bool = False  # Disabled by default until SAP credentials configured
    sap_host: str = ""  # SAP host (e.g., sap-system.example.com)
    sap_port: int = 44300  # SAP port (default 44300 for HTTP)
    sap_client_id: str = ""  # SAP OAuth client ID
    sap_client_secret: str = ""  # SAP OAuth client secret

    # Port-of-Entry (POE) geofencing for cross-border operations
    # Configurable POE geofences for customs visibility alerts
    # Format: {"poe_name": {"latitude": float, "longitude": float, "radius_meters": int}}
    # Example: Laredo, Otay Mesa, Eagle Pass for US-Mexico border
    poe_geofences: dict[str, dict[str, float | int]] = {}

    # Streams
    stream_inbound: str = "mandala:events"
    consumer_group: str = "mandala"

    # Throughput tuning (defaults conservative for small fleets)
    # Override these for high-volume deployments (1000+ trucks, high-frequency events)
    stream_batch_size: int = Field(default=10, ge=1, le=10000, description="Events per XREADGROUP batch")
    stream_block_ms: int = Field(default=5000, ge=0, le=60000, description="Block time in milliseconds for XREADGROUP")
    max_concurrent_events: int = Field(default=50, ge=1, le=1000, description="Max events processed concurrently per worker")
    stream_maxlen: int = Field(default=100_000, ge=1000, description="Max messages per Redis Stream (approximate)")

    # Backpressure handling (reject new events when overloaded)
    backpressure_enabled: bool = True
    backpressure_threshold: int = Field(default=80_000, ge=0, le=1_000_000, description="Stream length threshold for backpressure (80% of maxlen by default)")
    backpressure_response_code: int = Field(default=503, ge=400, le=599, description="HTTP status code when backpressure active")

    # Rate limiting (prevent abuse and protect against webhook floods)
    rate_limit_enabled: bool = True
    rate_limit_requests_per_minute: int = Field(default=1000, ge=1, le=100_000, description="Max requests per minute per IP")
    rate_limit_burst_size: int = Field(default=100, ge=1, le=10_000, description="Burst size for token bucket")

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

    # OpenTelemetry / OTLP span export (trace-native, Mandala 0.3+)
    # When set, every MandalaEvent is also shipped as an OTel span to the
    # configured OTLP/HTTP collector. Shipment-subjects auto-correlate into
    # a single trace. Zero overhead when empty.
    # Example: http://otel-collector:4318/v1/traces
    otlp_endpoint: str = ""
    otlp_service_name: str = "mandala"

    # Misc
    log_level: str = "INFO"
    state_ttl_seconds: int = 14 * 86_400

    # --- Apache Iceberg Event Log (Feature 1) ---
    # Permanent event storage on object storage (S3/GCS/Azure)
    # Separates ephemeral Redis Streams bus from permanent Iceberg log
    event_log_enabled: bool = False
    iceberg_catalog: str = "rest"  # rest, glue, hive, sql
    iceberg_catalog_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://mandala-events/"
    iceberg_table: str = "mandala.events"
    iceberg_namespace: str = "mandala"

    # --- Detector Sandbox (Feature 3) ---
    # Timeout and circuit breaker protection for detectors
    detector_sandbox_enabled: bool = True
    detector_timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    detector_circuit_breaker_threshold: int = Field(default=5, ge=1, le=50)
    detector_circuit_breaker_timeout: float = Field(default=60.0, ge=10.0, le=600.0)

    # --- Adaptive Backpressure (Feature 4) ---
    # Resource-aware backpressure based on system health
    adaptive_backpressure_enabled: bool = True
    redis_latency_threshold_ms: float = Field(default=100.0, ge=10.0, le=1000.0)
    memory_threshold_percent: float = Field(default=80.0, ge=50.0, le=95.0)
    cpu_threshold_percent: float = Field(default=80.0, ge=50.0, le=95.0)

    # --- Zero-Knowledge Proofs (Feature 2) ---
    # Privacy-preserving verification for insurance/customs/audits
    zk_enabled: bool = False
    zk_max_concurrent_proofs: int = 4
    zk_circuit_path: str = "/opt/mandala/zk/circuits/"
    zk_proving_key: str = "/opt/mandala/zk/keys/cold_chain_breach.pk"
    zk_verification_key: str = "/opt/mandala/zk/keys/cold_chain_breach.vk"
    zk_remote_verifier_endpoint: str = ""

    # --- Deterministic Event-Time Windowing (Feature 3) ---
    # Geometric Idempotency and Stator's Latch for out-of-order telemetry
    event_time_determinism_enabled: bool = True
    
    # Geometric hashing configuration
    geometric_hash_provider: str = "h3"  # h3, s2, or none
    geometric_hash_resolution: int = 9  # H3: 0-15, S2: 0-30, Geohash: 1-12
    
    # Stator's Latch configuration
    stator_latch_enabled: bool = True
    stator_latch_ttl_seconds: int = 14 * 86_400  # 14 days
    stator_latch_tolerance_seconds: int = 1  # Duplicate detection tolerance
    
    # Re-ordering Buffer configuration
    reorder_buffer_enabled: bool = True
    reorder_buffer_max_events_per_entity: int = 100
    reorder_buffer_max_wait_seconds: int = 300  # 5 minutes
    reorder_buffer_expire_seconds: int = 3600  # 1 hour
    reorder_buffer_check_interval_seconds: float = 5.0
    
    # Spatial coherence checks
    spatial_coherence_enabled: bool = True
    max_velocity_mps: float = 150.0  # ~335 mph, generous for trucks


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
