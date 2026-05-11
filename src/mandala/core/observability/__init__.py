"""Observability: trace-native export of Mandala events to OTLP backends.

Every :class:`MandalaEvent` is also an OpenTelemetry span. This subpackage
ships the optional OTLP exporter that turns the Redis Streams bus into a
trace producer for Jaeger, Tempo, Honeycomb, Datadog, Grafana Cloud, etc.

Activation is **opt-in** via the ``MANDALA_OTLP_ENDPOINT`` environment
variable (or :class:`mandala.settings.Settings.otlp_endpoint`). When unset
the exporter is a no-op and adds zero overhead.
"""
from mandala.core.observability.otlp_exporter import (
    OTLPExporter,
    get_exporter,
)

__all__ = ["OTLPExporter", "get_exporter"]
