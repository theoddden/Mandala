"""Event bus abstraction.

The default implementation uses Redis Streams with consumer groups for
exactly-once-ish processing (combined with the idempotency layer).
Production users can swap to Kafka/NATS by providing another :class:`EventBus`.
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterator, Protocol

import structlog

from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)


class EventBus(Protocol):
    """Pub/sub interface used everywhere inside Mandala."""

    async def publish(self, stream: str, event: MandalaEvent) -> str:
        """Append ``event`` to ``stream`` and return the assigned message id."""
        ...

    async def subscribe(
        self,
        stream: str,
        *,
        group: str,
        consumer: str,
        block_ms: int = 5000,
        count: int = 32,
    ) -> AsyncIterator[tuple[str, MandalaEvent]]:
        """Yield ``(message_id, event)`` tuples from ``stream``."""
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        ...


class RedisStreamsBus:
    """Production :class:`EventBus` backed by Redis Streams + consumer groups."""

    def __init__(self, redis: "object") -> None:
        self._redis = redis

    async def publish(self, stream: str, event: MandalaEvent) -> str:
        msg_id: str = await self._redis.xadd(  # type: ignore[attr-defined]
            stream, {"e": event.to_json()}, maxlen=100_000, approximate=True
        )
        return msg_id

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(  # type: ignore[attr-defined]
                name=stream, groupname=group, id="0", mkstream=True
            )
        except Exception as exc:  # noqa: BLE001
            # BUSYGROUP -> already exists, fine.
            if "BUSYGROUP" not in str(exc):
                raise

    async def subscribe(
        self,
        stream: str,
        *,
        group: str,
        consumer: str,
        block_ms: int = 5000,
        count: int = 32,
    ) -> AsyncIterator[tuple[str, MandalaEvent]]:
        await self._ensure_group(stream, group)
        while True:
            try:
                resp = await self._redis.xreadgroup(  # type: ignore[attr-defined]
                    groupname=group,
                    consumername=consumer,
                    streams={stream: ">"},
                    count=count,
                    block=block_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("redis.xreadgroup failed", stream=stream, group=group)
                await asyncio.sleep(1)
                continue

            if not resp:
                continue

            for _stream_name, messages in resp:
                for msg_id, fields in messages:
                    raw = fields.get(b"e") if isinstance(fields, dict) else fields.get("e")
                    if raw is None:
                        # Bad message; ack and skip so it doesn't poison the group.
                        await self.ack(stream, group, msg_id)
                        continue
                    try:
                        event = MandalaEvent.from_json(raw)
                    except Exception:
                        log.exception("malformed event on stream", stream=stream, msg_id=msg_id)
                        await self.ack(stream, group, msg_id)
                        continue
                    yield (msg_id if isinstance(msg_id, str) else msg_id.decode()), event

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(stream, group, message_id)  # type: ignore[attr-defined]
