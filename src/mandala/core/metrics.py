"""Prometheus metrics export for Mandala.

Exposes metrics for:
- Event processing throughput
- Detector execution latency
- Alert routing success/failure
- Dead letter queue backlog
- Redis stream lag
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Event processing metrics
events_processed_total = Counter(
    "mandala_events_processed_total",
    "Total number of events processed",
    ["event_type", "detector"]
)

events_processing_duration_seconds = Histogram(
    "mandala_events_processing_duration_seconds",
    "Event processing duration in seconds",
    ["event_type", "detector"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Detector metrics
detector_executions_total = Counter(
    "mandala_detector_executions_total",
    "Total number of detector executions",
    ["detector_name", "status"]  # status: success, failure
)

detector_execution_duration_seconds = Histogram(
    "mandala_detector_execution_duration_seconds",
    "Detector execution duration in seconds",
    ["detector_name"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Alert routing metrics
alerts_routed_total = Counter(
    "mandala_alerts_routed_total",
    "Total number of alerts routed to external channels",
    ["channel", "status"]  # channel: slack, email, pagerduty, samsara
)

alert_routing_duration_seconds = Histogram(
    "mandala_alert_routing_duration_seconds",
    "Alert routing duration in seconds",
    ["channel"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Dead letter queue metrics
dlq_size = Gauge(
    "mandala_dlq_size",
    "Current size of the dead letter queue"
)

dlq_events_total = Counter(
    "mandala_dlq_events_total",
    "Total number of events sent to dead letter queue",
    ["context"]  # context: projection, detector, webhook
)

# Redis stream metrics
stream_lag_seconds = Gauge(
    "mandala_stream_lag_seconds",
    "Lag between event time and processing time in seconds",
    ["stream"]
)

consumer_group_lag = Gauge(
    "mandala_consumer_group_lag",
    "Number of pending messages in consumer group",
    ["stream", "group"]
)

# Samsara outbound metrics
samsara_outbound_requests_total = Counter(
    "mandala_samsara_outbound_requests_total",
    "Total number of Samsara outbound API requests",
    ["endpoint", "status"]
)

samsara_outbound_duration_seconds = Histogram(
    "mandala_samsara_outbound_duration_seconds",
    "Samsara outbound API request duration in seconds",
    ["endpoint"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Load board metrics
loadboard_posts_total = Counter(
    "mandala_loadboard_posts_total",
    "Total number of load board posts",
    ["board", "status"]
)

loadboard_post_duration_seconds = Histogram(
    "mandala_loadboard_post_duration_seconds",
    "Load board post duration in seconds",
    ["board"],
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
)

# FMCSA enrichment metrics
fmcsa_fetches_total = Counter(
    "mandala_fmcsa_fetches_total",
    "Total number of FMCSA SAFER API fetches",
    ["status"]
)

fmcsa_fetch_duration_seconds = Histogram(
    "mandala_fmcsa_fetch_duration_seconds",
    "FMCSA SAFER API fetch duration in seconds",
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0)
)

# Rail enrichment metrics
rail_fetches_total = Counter(
    "mandala_rail_fetches_total",
    "Total number of Vizion API fetches",
    ["status"]
)

rail_fetch_duration_seconds = Histogram(
    "mandala_rail_fetch_duration_seconds",
    "Vizion API fetch duration in seconds",
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0)
)


# Materialized view metrics
view_apply_total = Counter(
    "mandala_view_apply_total",
    "Total number of view apply operations",
    ["view", "status"]
)
view_apply_duration_seconds = Histogram(
    "mandala_view_apply_duration_seconds",
    "Time spent applying events to views",
    ["view"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# Consumer group lag metrics
consumer_group_lag = Gauge(
    "mandala_consumer_group_lag",
    "Number of pending messages in consumer group",
    ["stream", "group"]
)


def start_metrics_server(port: int = 9090) -> None:
    """Start the Prometheus metrics HTTP server.

    Args:
        port: Port to serve metrics on (default: 9090)
    """
    start_http_server(port)
