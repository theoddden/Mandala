"""Kinaxis Maestro Connector - Redis Stream Subscription.

This connector subscribes to the Mandala Redis Streams bus and translates
MandalaEvent instances into Kinaxis Maestro disruption format for supply
chain planning and execution.

Integration with existing Mandala architecture:
- Subscribes to the Redis Streams bus (mandala:events)
- Translates events to Maestro disruption format
- Pushes to Kinaxis via HTTP client (stub implementation)
- Leverages existing MandalaEvent envelope and EventBus protocol

The MCP bridge is 80% there - this connector completes the integration by
handling the Kinaxis-specific translation and push logic.
"""
from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from pydantic import BaseModel, Field

from mandala.core.bus import EventBus, RedisStreamsBus
from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)


class MaestroDisruption(BaseModel):
    """A Kinaxis Maestro disruption event representation."""

    disruption_type: str = Field(description="Type of disruption (e.g., BORDER_DELAY, COLD_CHAIN_BREACH)")
    entity_id: str = Field(description="Entity identifier (shipment, truck, etc.)")
    entity_type: str = Field(description="Entity type (SHIPMENT, TRUCK, CARRIER)")
    severity: str = Field(description="Severity level (INFO, WARNING, CRITICAL)")
    timestamp: str = Field(description="RFC 3339 timestamp of the disruption")
    description: str = Field(description="Human-readable description")
    impact: dict[str, Any] = Field(description="Impact metrics (delay hours, cost, etc.)")
    source_system: str = Field(description="Source system that generated the disruption")
    correlation_id: str = Field(description="Correlation ID for traceability")


class KinaxisConnector:
    """Connector for pushing Mandala events to Kinaxis Maestro."""

    def __init__(
        self,
        bus: EventBus,
        kinaxis_api_url: str,
        kinaxis_api_key: str,
        batch_size: int = 50,
        flush_interval_sec: int = 30,
    ) -> None:
        self._bus = bus
        self._kinaxis_api_url = kinaxis_api_url
        self._kinaxis_api_key = kinaxis_api_key
        self._batch_size = batch_size
        self._flush_interval_sec = flush_interval_sec
        self._client = httpx.AsyncClient(
            base_url=kinaxis_api_url,
            headers={"X-Kinaxis-API-Key": kinaxis_api_key, "Content-Type": "application/json"},
            timeout=30.0,
        )
        self._batch: list[MaestroDisruption] = []
        self._running = False

    def _translate_to_maestro(self, event: MandalaEvent) -> MaestroDisruption | None:
        """Translate a MandalaEvent to a Maestro disruption."""
        event_type = event.type

        if event_type.startswith("mandala.border"):
            return self._translate_border_disruption(event)
        elif event_type.startswith("mandala.cold_chain"):
            return self._translate_cold_chain_disruption(event)
        elif event_type.startswith("mandala.customs"):
            return self._translate_customs_disruption(event)
        elif event_type.startswith("mandala.truck"):
            return self._translate_truck_disruption(event)
        elif event_type.startswith("mandala.shipment"):
            return self._translate_shipment_disruption(event)
        elif event_type.startswith("mandala.carrier"):
            return self._translate_carrier_disruption(event)
        else:
            log.debug("unmapped event type for Maestro", type=event_type)
            return None

    def _translate_border_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.border.crossing to BORDER_DELAY disruption."""
        truck_id = event.data.get("truck_id", "unknown")
        poe_code = event.data.get("poe_code", "unknown")
        
        # Calculate delay if customs filing is missing (alert condition)
        has_customs_filing = event.data.get("customs_filing_id") is not None
        severity = "INFO" if has_customs_filing else "CRITICAL"
        
        return MaestroDisruption(
            disruption_type="BORDER_DELAY",
            entity_id=truck_id,
            entity_type="TRUCK",
            severity=severity,
            timestamp=event.time.isoformat(),
            description=f"Border crossing at {poe_code}" + (
                " without customs filing - potential delay" if not has_customs_filing else " with customs filing"
            ),
            impact={
                "portOfEntry": poe_code,
                "customsFilingPresent": has_customs_filing,
                "detectionLagSeconds": (
                    (event.received_at - event.time).total_seconds()
                    if event.received_at and event.time
                    else None
                ),
            },
            source_system="MANDALA",
            correlation_id=event.id,
        )

    def _translate_cold_chain_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.cold_chain.breach to COLD_CHAIN_BREACH disruption."""
        shipment_id = event.data.get("shipment_id", "unknown")
        temperature = event.data.get("temperature", 0)
        declared_range = event.data.get("declared_range", {})
        
        return MaestroDisruption(
            disruption_type="COLD_CHAIN_BREACH",
            entity_id=shipment_id,
            entity_type="SHIPMENT",
            severity="CRITICAL",
            timestamp=event.time.isoformat(),
            description=f"Cold chain breach: temperature {temperature}°C outside range {declared_range}",
            impact={
                "temperature": temperature,
                "declaredRange": declared_range,
                "breachWindow": {
                    "start": event.data.get("breach_start"),
                    "end": event.data.get("breach_end"),
                },
                "regulatoryImpact": event.data.get("regulatory_impact"),
            },
            source_system="MANDALA",
            correlation_id=event.id,
        )

    def _translate_customs_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.customs.hold to CUSTOMS_HOLD disruption."""
        shipment_id = event.data.get("shipment_id", "unknown")
        hold_reason = event.data.get("hold_reason", "unknown")
        
        return MaestroDisruption(
            disruption_type="CUSTOMS_HOLD",
            entity_id=shipment_id,
            entity_type="SHIPMENT",
            severity="CRITICAL",
            timestamp=event.time.isoformat(),
            description=f"Customs hold: {hold_reason}",
            impact={
                "holdReason": hold_reason,
                "resolutionStatus": event.data.get("resolution_status"),
                "holdDurationHours": event.data.get("hold_duration_hours"),
            },
            source_system="DESCARTES",
            correlation_id=event.id,
        )

    def _translate_truck_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.truck.* events to TRUCK_AVAILABILITY disruption."""
        truck_id = event.data.get("truck_id", "unknown")
        
        # Check if this is an empty truck event (for load board posting)
        if event.data.get("empty", False):
            return MaestroDisruption(
                disruption_type="TRUCK_AVAILABILITY",
                entity_id=truck_id,
                entity_type="TRUCK",
                severity="INFO",
                timestamp=event.time.isoformat(),
                description=f"Truck {truck_id} is empty and available for load",
                impact={
                    "equipmentType": event.data.get("equipment_type"),
                    "gpsLocation": {
                        "latitude": event.data.get("latitude"),
                        "longitude": event.data.get("longitude"),
                    },
                    "carrierDot": event.data.get("carrier_dot"),
                },
                source_system="SAMSARA",
                correlation_id=event.id,
            )
        
        return None

    def _translate_shipment_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.shipment.* events to SHIPMENT_STATUS disruption."""
        shipment_id = event.data.get("shipment_id", "unknown")
        status = event.data.get("status", "unknown")
        
        # Only create disruptions for problematic statuses
        if status in ["DELAYED", "HELD", "CANCELLED", "EXCEPTION"]:
            return MaestroDisruption(
                disruption_type="SHIPMENT_STATUS",
                entity_id=shipment_id,
                entity_type="SHIPMENT",
                severity="WARNING",
                timestamp=event.time.isoformat(),
                description=f"Shipment status: {status}",
                impact={
                    "status": status,
                    "eta": event.data.get("eta"),
                    "origin": event.data.get("origin"),
                    "destination": event.data.get("destination"),
                },
                source_system="DESCARTES",
                correlation_id=event.id,
            )
        
        return None

    def _translate_carrier_disruption(self, event: MandalaEvent) -> MaestroDisruption:
        """Translate mandala.carrier.safety events to CARRIER_RISK disruption."""
        dot_number = event.data.get("dot_number", "unknown")
        csa_score = event.data.get("csa_score", 0)
        
        severity = "INFO"
        if csa_score > 75:
            severity = "WARNING"
        elif csa_score > 90:
            severity = "CRITICAL"
        
        return MaestroDisruption(
            disruption_type="CARRIER_RISK",
            entity_id=dot_number,
            entity_type="CARRIER",
            severity=severity,
            timestamp=event.time.isoformat(),
            description=f"Carrier safety score: {csa_score}",
            impact={
                "csaScore": csa_score,
                "inspectionHistory": event.data.get("inspection_history"),
                "authorityStatus": event.data.get("authority_status"),
            },
            source_system="FMCSA",
            correlation_id=event.id,
        )

    async def _push_to_kinaxis(self, disruptions: list[MaestroDisruption]) -> None:
        """Push a batch of disruptions to Kinaxis Maestro."""
        if not disruptions:
            return

        try:
            # Stub: In production, this would call the actual Kinaxis Maestro API
            # response = await self._client.post(
            #     "/api/disruptions/batch",
            #     json=[d.model_dump() for d in disruptions],
            # )
            # response.raise_for_status()

            log.info(
                "pushed disruptions to kinaxis (stub)",
                count=len(disruptions),
                disruption_types=[d.disruption_type for d in disruptions],
            )
        except httpx.HTTPError as exc:
            log.error("failed to push to kinaxis", error=str(exc))
            # In production, implement retry logic and dead-letter queue

    async def _flush_batch(self) -> None:
        """Flush the current batch to Kinaxis."""
        if self._batch:
            await self._push_to_kinaxis(self._batch)
            self._batch.clear()

    async def _process_event(self, msg_id: str, event: MandalaEvent) -> None:
        """Process a single MandalaEvent."""
        disruption = self._translate_to_maestro(event)
        if disruption:
            self._batch.append(disruption)

        if len(self._batch) >= self._batch_size:
            await self._flush_batch()

        # Ack the message so it doesn't get reprocessed
        await self._bus.ack("mandala:events", "kinaxis-consumer", msg_id)

    async def run(self) -> None:
        """Main loop: subscribe to events and push to Kinaxis."""
        self._running = True
        log.info("kinaxis connector starting")

        # Start background flush task
        flush_task = asyncio.create_task(self._flush_loop())

        try:
            async for msg_id, event in self._bus.subscribe(
                "mandala:events",
                group="kinaxis-consumer",
                consumer="kinaxis-worker-1",
            ):
                await self._process_event(msg_id, event)
        finally:
            self._running = False
            flush_task.cancel()
            await self._flush_batch()
            await self._client.aclose()
            log.info("kinaxis connector stopped")

    async def _flush_loop(self) -> None:
        """Background task to flush batch on interval."""
        while self._running:
            await asyncio.sleep(self._flush_interval_sec)
            await self._flush_batch()


async def main() -> None:
    """Entry point for running the Kinaxis connector."""
    import redis.asyncio as redis

    kinaxis_api_url = os.getenv("MANDALA_KINAXIS_API_URL")
    kinaxis_api_key = os.getenv("MANDALA_KINAXIS_API_KEY")
    redis_url = os.getenv("MANDALA_REDIS_URL", "redis://localhost:6379/0")

    if not kinaxis_api_url or not kinaxis_api_key:
        log.error("missing required env vars", kinaxis_api_url=bool(kinaxis_api_url), kinaxis_api_key=bool(kinaxis_api_key))
        return

    redis_client = await redis.from_url(redis_url, decode_responses=True)
    bus = RedisStreamsBus(redis_client)

    connector = KinaxisConnector(
        bus=bus,
        kinaxis_api_url=kinaxis_api_url,
        kinaxis_api_key=kinaxis_api_key,
        batch_size=int(os.getenv("MANDALA_KINAXIS_BATCH_SIZE", "50")),
        flush_interval_sec=int(os.getenv("MANDALA_KINAXIS_FLUSH_INTERVAL_SEC", "30")),
    )

    await connector.run()


if __name__ == "__main__":
    asyncio.run(main())
