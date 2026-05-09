"""The single Mandala worker process.

Consumes events from the inbound Redis stream, projects them into the
StateStore, runs detectors (alerts, load board auto-posting, enrichment),
and publishes any resulting events back to the stream.

This is the single Mandala worker process. Scale horizontally by running
multiple instances — they'll share the consumer group.
"""
from __future__ import annotations

import asyncio
import socket
import structlog

from datetime import datetime, timezone

import redis.asyncio as redis
import structlog

from mandala.alerts import DETECTORS as ALERT_DETECTORS
from mandala.connectors.samsara.outbound import SamsaraOutboundClient
from mandala.core.bus import RedisStreamsBus
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
    consumer = f"{socket.gethostname()}-{__import__('os').getpid()}"
    
    # Samsara outbound client (optional)
    samsara_outbound: SamsaraOutboundClient | None = None
    if s.samsara_outbound_enabled and s.samsara_api_token:
        samsara_outbound = SamsaraOutboundClient()
        log.info("samsara outbound integration enabled")

    log.info(
        "mandala.worker.start",
        stream=s.stream_inbound,
        consumer=consumer,
        group=s.consumer_group,
    )

    try:
        while True:
            messages = await bus.consume(
                stream=s.stream_inbound,
                group=s.consumer_group,
                consumer=consumer,
                count=10,
                block_ms=5000,
            )

            if not messages:
                continue

            for msg_id, event in messages:
                log.debug("mandala.worker.event", id=event.id, type=event.type)

                # Set processed_at timestamp for three-timestamp accounting
                event.processed_at = datetime.now(timezone.utc)

                # Project into StateStore
                await project(state, event)

                # Run detectors
                for detector in DETECTORS:
                    try:
                        new_events = await detector(event, state, r)
                        for ne in new_events:
                            # Set processed_at timestamp for three-timestamp accounting
                            ne.processed_at = datetime.now(timezone.utc)
                            await bus.publish(s.stream_inbound, ne)
                            
                            # Push alerts back to Samsara if enabled
                            if samsara_outbound and ne.type.startswith("mandala.alert"):
                                await _push_to_samsara(samsara_outbound, ne)
                    except Exception as exc:  # noqa: BLE001
                        log.exception(
                            "mandala.worker.detector_failed",
                            detector=detector.__name__,
                            event_id=event.id,
                            error=str(exc),
                        )

                await bus.ack(s.stream_inbound, group=s.consumer_group, id=msg_id)
    finally:
        if samsara_outbound:
            await samsara_outbound.close()
        await r.aclose()


async def _push_to_samsara(client: SamsaraOutboundClient, alert_event: dict) -> None:
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
