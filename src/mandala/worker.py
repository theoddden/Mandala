"""The single Mandala worker process.

Consumes events from the inbound Redis stream, projects them into the
StateStore, runs detectors (alerts, load board auto-posting, enrichment),
and publishes any resulting events back to the stream.

This is the single Mandala worker process. Scale horizontally by running
multiple instances — they'll share the consumer group.
"""
from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone

import redis.asyncio as redis
import structlog

from mandala.alerts import DETECTORS as ALERT_DETECTORS
from mandala.connectors.samsara.outbound import SamsaraOutboundClient
from mandala.core.alert_aggregation import AlertAggregator
from mandala.core.alert_routing import AlertRouter
from mandala.core.bus import RedisStreamsBus
from mandala.core.dead_letter import DeadLetterQueue
from mandala.core.events.envelope import MandalaEvent
from mandala.core.metrics import (
    alert_routing_duration_seconds,
    alerts_routed_total,
    detector_execution_duration_seconds,
    detector_executions_total,
    dlq_size,
    dlq_events_total,
    events_processed_total,
    events_processing_duration_seconds,
    start_metrics_server,
    stream_lag_seconds,
)
from mandala.core.state import StateStore
from mandala.fmcsa import DETECTORS as FMCSA_DETECTORS
from mandala.loadboard import DETECTORS as LOADBOARD_DETECTORS
from mandala.projection import project
from mandala.rail import DETECTORS as RAIL_DETECTORS
from mandala.settings import get_settings

DETECTORS = ALERT_DETECTORS + LOADBOARD_DETECTORS + FMCSA_DETECTORS + RAIL_DETECTORS

log = structlog.get_logger(__name__)


async def run() -> None:
    s = get_settings()
    r = redis.from_url(s.redis_url, decode_responses=False)
    bus = RedisStreamsBus(r)
    state = StateStore(r)
    dlq = DeadLetterQueue(r)
    alert_router = AlertRouter()
    alert_aggregator = AlertAggregator(r)
    consumer = f"{socket.gethostname()}-{os.getpid()}"
    
    # Samsara outbound client (optional)
    samsara_outbound: SamsaraOutboundClient | None = None
    if s.samsara_outbound_enabled and s.samsara_api_token:
        samsara_outbound = SamsaraOutboundClient()
        log.info("samsara outbound integration enabled")

    # Alert routing (optional)
    if s.alert_routing_enabled:
        log.info("alert routing enabled")

    # Alert aggregation (optional)
    if s.alert_aggregation_enabled:
        log.info("alert aggregation enabled")

    # Start Prometheus metrics server (optional)
    if s.metrics_enabled:
        start_metrics_server(s.metrics_port)
        log.info("metrics server started", port=s.metrics_port)

    log.info(
        "mandala.worker.start",
        stream=s.stream_inbound,
        consumer=consumer,
        group=s.consumer_group,
    )

    # Throttle DLQ size metric updates to avoid N+1 round trips per event.
    DLQ_STATS_INTERVAL_SEC = 30.0
    last_dlq_stats_at = 0.0

    try:
        while True:
            messages = await bus.consume(
                s.stream_inbound,
                group=s.consumer_group,
                consumer=consumer,
                count=10,
                block_ms=5000,
            )

            if not messages:
                continue

            for msg_id, event in messages:
                event_start = datetime.now(timezone.utc)
                log.debug("mandala.worker.event", id=event.id, type=event.type)

                # Set processed_at timestamp for three-timestamp accounting
                event.processed_at = datetime.now(timezone.utc)

                # Calculate stream lag (event time to processing time)
                if event.time:
                    lag_seconds = (event.processed_at - event.time).total_seconds()
                    stream_lag_seconds.labels(stream=s.stream_inbound).set(lag_seconds)

                # Project into StateStore (with DLQ fallback)
                try:
                    await project(event, state)
                except Exception as exc:  # noqa: BLE001
                    log.exception("mandala.worker.projection_failed", event_id=event.id)
                    await dlq.publish(event, str(exc), "projection")
                    dlq_events_total.labels(context="projection").inc()
                    # Skip detectors when projection fails: detectors read state
                    # that wasn't updated, so their results would be inconsistent.
                    await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
                    continue

                # Run detectors (with DLQ fallback)
                for detector in DETECTORS:
                    detector_start = datetime.now(timezone.utc)
                    try:
                        new_events = await detector(event, state, r)
                        detector_duration = (datetime.now(timezone.utc) - detector_start).total_seconds()
                        detector_execution_duration_seconds.labels(detector_name=detector.__name__).observe(detector_duration)
                        detector_executions_total.labels(detector_name=detector.__name__, status="success").inc()
                        
                        for ne in new_events:
                            # Set processed_at timestamp for three-timestamp accounting
                            ne.processed_at = datetime.now(timezone.utc)
                            # Detector-emitted events have a fresh `time` per
                            # invocation, so the bus-layer dedup key would never
                            # match. Disable it here; webhook layer is the
                            # authoritative dedup boundary for ingest.
                            published_id = await bus.publish(
                                s.stream_inbound, ne, enable_deduplication=False
                            )
                            if not published_id:
                                # Dropped as duplicate — don't inflate metrics.
                                continue
                            events_processed_total.labels(event_type=ne.type, detector=detector.__name__).inc()
                            
                            # Push alerts back to Samsara if enabled
                            if samsara_outbound and ne.type.startswith("mandala.alert"):
                                await _push_to_samsara(samsara_outbound, ne)
                            
                            # Route alerts to external channels if enabled
                            if s.alert_routing_enabled and ne.type.startswith("mandala.alert"):
                                # Check aggregation before routing
                                if await alert_aggregator.should_route(ne):
                                    route_start = datetime.now(timezone.utc)
                                    await alert_router.route(ne)
                                    route_duration = (datetime.now(timezone.utc) - route_start).total_seconds()
                                    alert_routing_duration_seconds.labels(channel="external").observe(route_duration)
                                    alerts_routed_total.labels(channel="external", status="success").inc()
                                else:
                                    log.debug(
                                        "alert.aggregated.skipped_routing",
                                        alert_type=ne.type,
                                    )
                    except Exception as exc:  # noqa: BLE001
                        detector_duration = (datetime.now(timezone.utc) - detector_start).total_seconds()
                        detector_execution_duration_seconds.labels(detector_name=detector.__name__).observe(detector_duration)
                        detector_executions_total.labels(detector_name=detector.__name__, status="failure").inc()
                        log.exception(
                            "mandala.worker.detector_failed",
                            detector=detector.__name__,
                            event_id=event.id,
                            error=str(exc),
                        )
                        await dlq.publish(
                            event,
                            str(exc),
                            "detector",
                            metadata={"detector": detector.__name__},
                        )
                        dlq_events_total.labels(context="detector").inc()

                event_duration = (datetime.now(timezone.utc) - event_start).total_seconds()
                events_processing_duration_seconds.labels(event_type=event.type, detector="all").observe(event_duration)

                # Update DLQ size metric at most once every DLQ_STATS_INTERVAL_SEC
                # to avoid two extra Redis round trips per event.
                now_mono = asyncio.get_event_loop().time()
                if now_mono - last_dlq_stats_at >= DLQ_STATS_INTERVAL_SEC:
                    last_dlq_stats_at = now_mono
                    dlq_stats = await dlq.stats()
                    if "length" in dlq_stats:
                        dlq_size.set(dlq_stats["length"])

                await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
    finally:
        if samsara_outbound:
            await samsara_outbound.close()
        await alert_router.close()
        await r.aclose()


async def _push_to_samsara(
    client: SamsaraOutboundClient, alert_event: MandalaEvent
) -> None:
    """Push Mandala alerts back to Samsara.
    
    Creates Samsara alerts and updates custom fields for:
    - Border crossing without customs filing
    - Cold chain breaches
    - Carrier safety issues
    """
    truck_id = alert_event.data.get("truck_id")
    if not truck_id:
        return
    
    alert_type = alert_event.type
    severity = alert_event.data.get("severity", "WARNING")
    reason = alert_event.data.get("reason", "")
    
    # Map Mandala alert types to Samsara alert types
    samsara_alert_type = "MANDALA_ENRICHMENT"
    if "border" in alert_type:
        samsara_alert_type = "CUSTOMS_COMPLIANCE"
    elif "cold_chain" in alert_type:
        samsara_alert_type = "COLD_CHAIN"
    elif "carrier" in alert_type:
        samsara_alert_type = "CARRIER_SAFETY"
    
    # Create alert in Samsara
    await client.create_alert(
        vehicle_id=truck_id,
        alert_type=samsara_alert_type,
        severity=severity,
        message=f"Mandala Alert: {reason}",
    )
    
    # Update custom field with alert status
    await client.update_custom_field(
        vehicle_id=truck_id,
        field_id="mandala_alert_status",
        value=f"{alert_type}:{severity}",
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
