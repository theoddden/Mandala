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
import signal
import socket
import time as _time
from collections import defaultdict
from datetime import UTC, datetime

import redis.asyncio as redis
import structlog

from mandala.alerts import DETECTORS as ALERT_DETECTORS
from mandala.connectors.samsara.outbound import SamsaraOutboundClient
from mandala.core.adaptive_backpressure import AdaptiveBackpressure
from mandala.core.alert_aggregation import AlertAggregator
from mandala.core.alert_routing import AlertRouter
from mandala.core.bus import RedisStreamsBus
from mandala.core.compliance.change_tracker import ChangeTracker
from mandala.core.compliance.pii_detector import PIIDetector
from mandala.core.dead_letter import DeadLetterQueue
from mandala.core.detector_sandbox import DetectorSandboxPool
from mandala.core.events.envelope import MandalaEvent
from mandala.core.geometric_hash import GeometricHashProvider, GeometricHashService
from mandala.core.metrics import (
    alert_routing_duration_seconds,
    alerts_routed_total,
    detector_execution_duration_seconds,
    dlq_events_total,
    events_processed_total,
    events_processing_duration_seconds,
    start_metrics_server,
    stream_lag_seconds,
)
from mandala.core.observability import get_exporter
from mandala.core.reorder_buffer import ReorderBufferManager
from mandala.core.state import StateStore
from mandala.core.stator_latch import LatchDecision, StatorLatch
from mandala.core.zk.proving_service import AsyncProvingService
from mandala.detectors.warehouse import DETECTORS as WAREHOUSE_DETECTORS
from mandala.fmcsa import DETECTORS as FMCSA_DETECTORS
from mandala.loadboard import DETECTORS as LOADBOARD_DETECTORS
from mandala.projection import project
from mandala.rail import DETECTORS as RAIL_DETECTORS
from mandala.settings import get_settings

# Compliance detectors (optional, enabled via settings)
COMPLIANCE_DETECTORS = []

s = get_settings()
if s.pii_detection_enabled:
    pii_detector = PIIDetector(enabled=True)
    COMPLIANCE_DETECTORS.append(pii_detector)
if s.change_tracking_enabled:
    change_tracker = ChangeTracker(enabled=True)
    COMPLIANCE_DETECTORS.append(change_tracker)

DETECTORS = (
    ALERT_DETECTORS
    + LOADBOARD_DETECTORS
    + FMCSA_DETECTORS
    + RAIL_DETECTORS
    + WAREHOUSE_DETECTORS
    + COMPLIANCE_DETECTORS
)

log = structlog.get_logger(__name__)

# Global shutdown flag for graceful shutdown
_shutdown_requested = False


def _request_shutdown(signum, frame):
    """Signal handler for graceful shutdown."""
    global _shutdown_requested
    _shutdown_requested = True
    log.info("mandala.worker.shutdown_requested", signal=signum)


async def _probe_redis_version(redis: object) -> str:
    """Probe Redis server version at startup for feature detection."""
    try:
        info = await redis.info("server")  # type: ignore[attr-defined]
        # redis-py returns dict; version is in info['redis_version']
        version = info.get("redis_version", "unknown") if isinstance(info, dict) else "unknown"
        log.info("redis_version_probe", version=version)
        return str(version)
    except Exception as exc:  # noqa: BLE001
        log.exception("redis_version_probe_failed", error=str(exc))
        return "unknown"


async def run() -> None:
    global _shutdown_requested
    s = get_settings()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    # Use connection pool for better performance under high throughput
    pool = redis.ConnectionPool.from_url(s.redis_url, decode_responses=False)
    r = redis.Redis(connection_pool=pool)

    # Probe Redis version at startup for feature detection
    await _probe_redis_version(r)

    bus = RedisStreamsBus(r)
    state = StateStore(r)
    dlq = DeadLetterQueue(r)
    alert_router = AlertRouter()
    alert_aggregator = AlertAggregator(r)
    consumer = f"{socket.gethostname()}-{os.getpid()}"

    # Deterministic Event-Time Windowing components
    stator_latch: StatorLatch | None = None
    reorder_buffer_manager: ReorderBufferManager | None = None
    geo_hash_service: GeometricHashService | None = None

    if s.event_time_determinism_enabled:
        # Initialize Stator's Latch for event-time determinism
        if s.stator_latch_enabled:
            stator_latch = StatorLatch(r, ttl_seconds=s.stator_latch_ttl_seconds)
            log.info("stator_latch.enabled", ttl_seconds=s.stator_latch_ttl_seconds)

        # Initialize Re-ordering Buffer for out-of-order events
        if s.reorder_buffer_enabled:

            async def _reorder_release_callback(released_event: MandalaEvent) -> None:
                await bus.publish(s.stream_inbound, released_event, enable_deduplication=False)

            reorder_buffer_manager = ReorderBufferManager(
                redis=r,
                on_release=_reorder_release_callback,
                max_events_per_entity=s.reorder_buffer_max_events_per_entity,
                max_wait_seconds=s.reorder_buffer_max_wait_seconds,
                expire_seconds=s.reorder_buffer_expire_seconds,
            )
            await reorder_buffer_manager.start(s.reorder_buffer_check_interval_seconds)
            log.info(
                "reorder_buffer.enabled",
                max_events=s.reorder_buffer_max_events_per_entity,
                max_wait_seconds=s.reorder_buffer_max_wait_seconds,
            )

        # Initialize Geometric Hash Service
        geo_hash_service = GeometricHashService(
            provider=GeometricHashProvider(s.geometric_hash_provider),
            resolution=s.geometric_hash_resolution,
        )
        log.info(
            "geometric_hash.enabled",
            provider=s.geometric_hash_provider,
            resolution=s.geometric_hash_resolution,
        )

    # Detector sandbox for timeout and circuit breaker protection
    detector_sandbox = DetectorSandboxPool(DETECTORS)

    # Adaptive backpressure for resource-aware processing
    adaptive_backpressure = AdaptiveBackpressure(r)

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

    # Start OTLP exporter (opt-in; no-op when MANDALA_OTLP_ENDPOINT unset)
    otlp = get_exporter()
    await otlp.start()
    if otlp.enabled:
        log.info("otlp exporter enabled", endpoint=otlp.endpoint)

    # Start ZK proving service (if enabled)
    proving_service: AsyncProvingService | None = None
    if s.zk_enabled:
        proving_service = AsyncProvingService(max_concurrent_proofs=s.zk_max_concurrent_proofs, redis=r)
        await proving_service.start()
        log.info("zk.proving_service.enabled", max_concurrent=s.zk_max_concurrent_proofs)

    log.info(
        "mandala.worker.start",
        stream=s.stream_inbound,
        consumer=consumer,
        group=s.consumer_group,
    )

    # Throttle DLQ size metric updates to avoid N+1 round trips per event.
    DLQ_STATS_INTERVAL_SEC = 30.0
    last_dlq_stats_at = 0.0

    # Concurrency limiter for batch processing (backpressure control)
    # Prevents unbounded in-flight work that could exhaust memory/connections
    event_semaphore = asyncio.Semaphore(s.max_concurrent_events)

    # -----------------------------------------------------------------------
    # Define entity processor once (outside the main loop) so the closure is
    # not re-created on every batch iteration.
    # -----------------------------------------------------------------------
    async def process_entity_events(entity_id: str, entity_events: list[tuple[str, MandalaEvent]]) -> None:
        # Process events for this entity sequentially to preserve ordering
        # and prevent read-modify-write race conditions on state
        for msg_id, event in entity_events:
            # Concurrency limiter (backpressure control)
            async with event_semaphore:
                event_start = datetime.now(UTC)
                log.debug("mandala.worker.event", id=event.id, type=event.type, entity=entity_id)

                # Set processed_at timestamp for three-timestamp accounting
                event.processed_at = datetime.now(UTC)

                # --- Deterministic Event-Time Windowing ---
                # Extract coordinates and compute geometric hash if available
                latitude = None
                longitude = None
                if event.data and isinstance(event.data, dict):
                    latitude = event.data.get("latitude")
                    longitude = event.data.get("longitude")
                    # Also check nested location objects
                    if latitude is None:
                        location = event.data.get("location", {})
                        if isinstance(location, dict):
                            latitude = location.get("latitude")
                            longitude = location.get("longitude")

                # Compute geometric hash if coordinates available and service enabled
                if geo_hash_service and latitude is not None and longitude is not None:
                    event.geometric_hash = geo_hash_service.compute_hash(latitude, longitude, event.time)
                    log.debug(
                        "geometric_hash.computed",
                        event_id=event.id,
                        hash=event.geometric_hash,
                        lat=latitude,
                        lon=longitude,
                    )

                # Check Stator's Latch for event-time determinism
                latch_decision = LatchDecision.PROCEED
                if stator_latch and event.time:
                    latch_result = await stator_latch.check(
                        source_id=entity_id,
                        event_time=event.time,
                        geometric_hash=event.geometric_hash,
                        tolerance_seconds=s.stator_latch_tolerance_seconds,
                    )
                    latch_decision = latch_result.decision

                    if latch_decision == LatchDecision.BACKFILL:
                        # Time-travel data: backfill historical graph, bypass real-time Turbine
                        log.info(
                            "stator_latch.backfill",
                            event_id=event.id,
                            entity=entity_id,
                            event_time=event.time,
                            lag_seconds=latch_result.metadata.get("lag_seconds"),
                        )
                        # Acknowledge and skip detectors (backfill only)
                        await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
                        continue
                    if latch_decision == LatchDecision.DUPLICATE:
                        # Duplicate event: drop and acknowledge
                        log.debug(
                            "stator_latch.duplicate",
                            event_id=event.id,
                            entity=entity_id,
                        )
                        await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
                        continue

                # Check re-ordering buffer for out-of-order events
                if reorder_buffer_manager and event.time:
                    should_release, buffered_event = await reorder_buffer_manager.add_event(
                        event=event,
                        source_id=entity_id,
                        event_time=event.time,
                    )
                    if not should_release:
                        # Event is buffered for later release
                        log.debug(
                            "reorder_buffer.buffered",
                            event_id=event.id,
                            entity=entity_id,
                            event_time=event.time,
                        )
                        # Acknowledge the stream message (event is in buffer)
                        await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
                        continue
                    # If buffered_event is not None, use that instead (re-ordered)
                    if buffered_event:
                        event = buffered_event

                # Trace-native: ship every ingested event as an OTel span.
                # No-op when MANDALA_OTLP_ENDPOINT is unset.
                otlp.emit(event)

                # Calculate stream lag (event time to processing time)
                if event.time:
                    lag_seconds = (event.processed_at - event.time).total_seconds()
                    stream_lag_seconds.labels(stream=s.stream_inbound).set(lag_seconds)

                # Project into StateStore (with DLQ fallback)
                try:
                    await project(event, state)
                except Exception as exc:  # noqa: BLE001
                    log.exception("mandala.worker.projection_failed", event_id=event.id, entity=entity_id)
                    await dlq.publish(event, str(exc), "projection")
                    dlq_events_total.labels(context="projection").inc()
                    # Skip detectors when projection fails: detectors read state
                    # that wasn't updated, so their results would be inconsistent.
                    await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
                    continue

                # Run detectors in parallel with sandbox protection
                # Sandbox provides timeout and circuit breaker protection
                detector_start = datetime.now(UTC)
                try:
                    new_events = await detector_sandbox.execute_all(event, state, r)
                    detector_duration = (datetime.now(UTC) - detector_start).total_seconds()
                    detector_execution_duration_seconds.labels(detector_name="all").observe(detector_duration)

                    for ne in new_events:
                        # Set processed_at timestamp for three-timestamp accounting
                        ne.processed_at = datetime.now(UTC)
                        # Trace-native: link detector-emitted spans to the
                        # ingest span (causal parent) so the OTel trace shows
                        # the full causality chain in any backend.
                        if ne.parent_span_id is None and event.span_id:
                            ne.parent_span_id = event.span_id
                        otlp.emit(ne)
                        # Detector-emitted events have a fresh `time` per
                        # invocation, so the bus-layer dedup key would never
                        # match. Disable it here; webhook layer is the
                        # authoritative dedup boundary for ingest.
                        published_id = await bus.publish(s.stream_inbound, ne, enable_deduplication=False)
                        if not published_id:
                            # Dropped as duplicate — don't inflate metrics.
                            continue
                        events_processed_total.labels(event_type=ne.type, detector="sandbox").inc()

                        # Mirror alerts to dedicated stream for O(1) MCP queries
                        if ne.type.startswith("mandala.alert"):
                            await r.xadd(  # type: ignore[union-attr]
                                "mandala:alerts",
                                {"e": ne.to_json()},
                                maxlen=s.alerts_stream_maxlen,
                                approximate=True,
                            )

                        # Push alerts back to Samsara if enabled
                        if samsara_outbound and ne.type.startswith("mandala.alert"):
                            await _push_to_samsara(samsara_outbound, ne)

                        # Route alerts to external channels if enabled
                        if s.alert_routing_enabled and ne.type.startswith("mandala.alert"):
                            # Check aggregation before routing
                            if await alert_aggregator.should_route(ne):
                                route_start = datetime.now(UTC)
                                await alert_router.route(ne)
                                route_duration = (datetime.now(UTC) - route_start).total_seconds()
                                alert_routing_duration_seconds.labels(channel="external").observe(route_duration)
                                alerts_routed_total.labels(channel="external", status="success").inc()
                            else:
                                log.debug(
                                    "alert.aggregated.skipped_routing",
                                    alert_type=ne.type,
                                )

                        # Enqueue ZK proof generation for cold-chain breaches (if enabled)
                        if proving_service and ne.type == "mandala.alert.cold_chain.out_of_spec":
                            data = ne.data if isinstance(ne.data, dict) else {}
                            await proving_service.enqueue_proof_request(
                                event=ne,
                                proof_params={
                                    "declared_min_c": data.get("declared_min_c", 2.0),
                                    "declared_max_c": data.get("declared_max_c", 8.0),
                                    "breach_timestamp": ne.time,
                                },
                            )
                            log.info("zk.proof.auto_enqueued", event_id=ne.id)
                except Exception as exc:  # noqa: BLE001
                    detector_duration = (datetime.now(UTC) - detector_start).total_seconds()
                    detector_execution_duration_seconds.labels(detector_name="sandbox").observe(detector_duration)
                    log.exception(
                        "mandala.worker.detector_sandbox_failed",
                        event_id=event.id,
                        error=str(exc),
                    )
                    await dlq.publish(
                        event,
                        str(exc),
                        "detector",
                        metadata={"context": "sandbox"},
                    )
                    dlq_events_total.labels(context="detector").inc()

                event_duration = (datetime.now(UTC) - event_start).total_seconds()
                events_processing_duration_seconds.labels(event_type=event.type, detector="all").observe(event_duration)

                # Acknowledge event after successful processing
                await bus.ack(s.stream_inbound, s.consumer_group, msg_id)

    # PEL reclaim: track last reclaim time for periodic XAUTOCLAIM
    _last_reclaim_at: float = 0.0
    _PEL_RECLAIM_INTERVAL_SEC: float = 60.0

    try:
        while not _shutdown_requested:
            # --- Adaptive batch size (Fix 8) ---
            batch_size = s.stream_batch_size
            if s.adaptive_backpressure_enabled:
                health = await adaptive_backpressure.check_health()
                batch_size = adaptive_backpressure.adapt_batch_size(health)

            messages = await bus.consume(
                s.stream_inbound,
                group=s.consumer_group,
                consumer=consumer,
                count=batch_size,
                block_ms=s.stream_block_ms,
            )

            # --- Periodic PEL reclaim (Fix 14) ---
            _now = _time.monotonic()
            if _now - _last_reclaim_at >= _PEL_RECLAIM_INTERVAL_SEC:
                _last_reclaim_at = _now
                try:
                    reclaimed = await bus.reclaim_pending(
                        s.stream_inbound,
                        group=s.consumer_group,
                        consumer=consumer,
                        min_idle_ms=max(s.stream_block_ms * 2, 30_000),
                    )
                    if reclaimed:
                        log.info("worker.reclaimed_pending_entries", count=len(reclaimed))
                        reclaimed_by_entity: dict[str, list[tuple[str, MandalaEvent]]] = defaultdict(list)
                        for msg_id, ev in reclaimed:
                            reclaimed_by_entity[ev.subject or ev.id].append((msg_id, ev))
                        await asyncio.gather(
                            *[process_entity_events(eid, evts) for eid, evts in reclaimed_by_entity.items()],
                            return_exceptions=True,
                        )
                except Exception:  # noqa: BLE001
                    log.exception("worker.reclaim_error")

            if not messages:
                continue

            # Group events by entity ID (subject) to prevent race conditions
            # Events for the same entity process sequentially (preserves ordering)
            # Events for different entities process in parallel (throughput)
            events_by_entity: dict[str, list[tuple[str, MandalaEvent]]] = defaultdict(list)
            for msg_id, event in messages:
                # Use subject as entity ID; if no subject, use event ID
                entity_id = event.subject or event.id
                events_by_entity[entity_id].append((msg_id, event))

            # Process entities in parallel, events within each entity sequentially
            # return_exceptions=True ensures one entity failure doesn't cancel others
            await asyncio.gather(
                *[process_entity_events(entity_id, events) for entity_id, events in events_by_entity.items()],
                return_exceptions=True,
            )
    finally:
        log.info("mandala.worker.shutdown", reason="signal_or_exception")
        if samsara_outbound:
            await samsara_outbound.close()
        await alert_router.close()
        await otlp.stop()
        if proving_service:
            await proving_service.stop()
        if reorder_buffer_manager:
            await reorder_buffer_manager.stop()
        await r.aclose()
        log.info("mandala.worker.shutdown_complete")


async def _push_to_samsara(client: SamsaraOutboundClient, alert_event: MandalaEvent) -> None:
    """Push Mandala alerts back to Samsara.

    Sends driver messages and updates vehicle tags for:
    - Border crossing without customs filing
    - Cold chain breaches
    """
    data = alert_event.data
    truck_id = data.get("truck_id")
    alert_type = data.get("alert_type")
    severity = data.get("severity", "WARNING")

    if not truck_id or not alert_type:
        log.warning(
            "samsara.push.missing_fields",
            truck_id=truck_id,
            alert_type=alert_type,
        )
        return

    try:
        # Send driver message with alert details
        await client.send_driver_message(
            driver_id=truck_id,
            message=f"Mandala Alert ({severity}): {alert_type} - {data.get('message', '')}",
        )

        # Update vehicle tag with alert status
        await client.update_vehicle_tag(
            vehicle_id=truck_id,
            tag_key="mandala_alert_status",
            tag_value=f"{alert_type}:{severity}",
        )

        log.info(
            "samsara.push.success",
            truck_id=truck_id,
            alert_type=alert_type,
            severity=severity,
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "samsara.push.failed",
            truck_id=truck_id,
            alert_type=alert_type,
            error=str(exc),
        )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
