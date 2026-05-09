"""The single Mandala worker.

Reads the inbound stream, projects events into state, runs alert detectors,
publishes any resulting alerts back onto the same stream so they flow to
the warehouse sink and any external subscribers.

One process. No celery. No multiple worker types. If you outgrow this,
fork the loop and shard by stream key.
"""
from __future__ import annotations

import asyncio
import socket

import redis.asyncio as redis
import structlog

from mandala.alerts import DETECTORS as ALERT_DETECTORS
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
    log.info("mandala.worker.start", stream=s.stream_inbound, consumer=consumer)

    try:
        async for msg_id, event in bus.subscribe(
            s.stream_inbound, group=s.consumer_group, consumer=consumer
        ):
            try:
                await project(event, state)
                for detector in DETECTORS:
                    for alert in await detector(event, state, r):
                        await bus.publish(s.stream_inbound, alert)
            except Exception:  # noqa: BLE001
                log.exception("worker.process_failed", event_id=event.id, type=event.type)
            finally:
                await bus.ack(s.stream_inbound, s.consumer_group, msg_id)
    finally:
        await r.aclose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
