"""Sink runner — reads the inbound stream and pushes batches to a :class:`Sink`.

Lives in its own consumer group so it doesn't compete with the alert
worker. Run via ``mandala sink --root ./warehouse``.
"""
from __future__ import annotations

import asyncio
import socket

import redis.asyncio as redis
import structlog

from mandala.core.bus import RedisStreamsBus
from mandala.settings import get_settings
from mandala.sinks.base import SinkRecord
from mandala.sinks.jsonl import JsonlFileSink

log = structlog.get_logger(__name__)

SINK_GROUP = "mandala-sink"
BATCH_SIZE = 200
FLUSH_INTERVAL_SECONDS = 5.0


async def run(root: str) -> None:
    s = get_settings()
    r = redis.from_url(s.redis_url, decode_responses=False)
    bus = RedisStreamsBus(r)
    sink = JsonlFileSink(root)
    consumer = f"sink-{socket.gethostname()}-{__import__('os').getpid()}"
    log.info("sink.runner.start", root=root, stream=s.stream_inbound)

    buffer: list[tuple[str, SinkRecord]] = []
    last_flush = asyncio.get_running_loop().time()

    async def flush() -> None:
        nonlocal buffer, last_flush
        if not buffer:
            return
        await sink.write_batch([rec for _, rec in buffer])
        for msg_id, _ in buffer:
            await bus.ack(s.stream_inbound, SINK_GROUP, msg_id)
        buffer = []
        last_flush = asyncio.get_running_loop().time()

    try:
        async for msg_id, event in bus.subscribe(
            s.stream_inbound, group=SINK_GROUP, consumer=consumer, block_ms=1000
        ):
            buffer.append((msg_id, SinkRecord.from_event(event)))
            now = asyncio.get_running_loop().time()
            if len(buffer) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL_SECONDS:
                await flush()
    finally:
        await flush()
        await sink.aclose()
        await r.aclose()


def main() -> None:
    import sys

    root = sys.argv[1] if len(sys.argv) > 1 else "./warehouse"
    asyncio.run(run(root))


if __name__ == "__main__":
    main()
