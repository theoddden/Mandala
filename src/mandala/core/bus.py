"""Event bus abstraction.

The default implementation uses Redis Streams with consumer groups for
exactly-once-ish processing (combined with the idempotency layer).
Production users can swap to Kafka/NATS by providing another :class:`EventBus`.

Exactly-once delivery is achieved via idempotency keys derived from
SHA256(vendor + event_type + occurred_at + entity_id). Before publishing
to the stream, the key is checked against a Redis SET with 14-day TTL.
If the key exists, the event is dropped (duplicate). This atomic check-and-publish
is performed via a Lua script to prevent race conditions.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Protocol

import structlog

from mandala.core.event_log import EventLog
from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings

log = structlog.get_logger(__name__)

# Lua script for atomic deduplication check-and-publish
# Returns 0 if key exists (duplicate), 1 if key was added (new event)
DEDUPE_SCRIPT = """
local key = KEYS[1]
local ttl = ARGV[1]
if redis.call('EXISTS', key) == 1 then
    return 0
end
redis.call('SETEX', key, ttl, '1')
return 1
"""


class EventBus(Protocol):
    """Pub/sub interface used everywhere inside Mandala."""

    async def publish(self, stream: str, event: MandalaEvent, *, enable_deduplication: bool = True) -> str:
        """Append ``event`` to ``stream`` and return the assigned message id.

        Args:
            stream: Redis stream name
            event: MandalaEvent to publish
            enable_deduplication: If True, check idempotency key before publishing
        """
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

    async def consume(
        self,
        stream: str,
        *,
        group: str,
        consumer: str,
        count: int = 32,
        block_ms: int = 5000,
    ) -> list[tuple[str, MandalaEvent]]:
        """One-shot batched read; returns at most ``count`` events."""
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> None: ...


class EventProcessor:
    """Processes events from the event bus.

    Can filter by event type and invoke a handler function.
    """

    def __init__(self, handler: object, filter_types: list[str] | None = None) -> None:
        """Initialize the event processor.

        Args:
            handler: Async callable to handle events
            filter_types: Optional list of event types to filter (if None, processes all)
        """
        self._handler = handler
        self._filter_types = filter_types or []

    async def process(self, event: MandalaEvent) -> bool:
        """Process an event if it matches the filter.

        Args:
            event: The event to process

        Returns:
            True if the event was processed, False if it was filtered out
        """
        if self._filter_types and event.type not in self._filter_types:
            return False

        if self._handler:
            result = await self._handler(event)
            return result if result is not None else True

        return True


class RedisStreamsBus:
    """Production :class:`EventBus` backed by Redis Streams + consumer groups.

    Supports dual-write to Iceberg for permanent event log storage.
    """

    # 14-day TTL for idempotency keys (matches StateStore TTL)
    IDEMPOTENCY_TTL_SEC = 14 * 24 * 60 * 60  # 14 days in seconds

    def __init__(self, redis: object, event_log: EventLog | None = None) -> None:
        self._redis = redis
        self._dedupe_script_sha: str | None = None
        self._event_log = event_log  # Optional Iceberg event log for dual-write

    async def _ensure_dedupe_script(self) -> None:
        """Register the deduplication Lua script if not already registered."""
        if self._dedupe_script_sha is not None:
            return

        try:
            self._dedupe_script_sha = await self._redis.script_load(DEDUPE_SCRIPT)  # type: ignore[attr-defined]
            log.info("deduplication script registered", sha=self._dedupe_script_sha)
        except Exception as exc:  # noqa: BLE001
            log.exception("failed to register deduplication script", error=str(exc))
            raise

    async def publish(self, stream: str, event: MandalaEvent, *, enable_deduplication: bool = True) -> str:
        """Append ``event`` to ``stream`` and return the assigned message id.

        If enable_deduplication is True, checks idempotency key before publishing
        to prevent duplicate events from webhook retries or network hiccups.
        """
        # Compute idempotency key if not already set
        if not event.mandalaidempotencykey:
            event.mandalaidempotencykey = event.compute_idempotency_key()

        # Check deduplication if enabled
        if enable_deduplication:
            await self._ensure_dedupe_script()

            dedupe_key = f"mandala:idempotency:{event.mandalaidempotencykey}"

            # Execute deduplication script atomically via EVALSHA with EVAL fallback.
            # EVALSHA is faster (sends only SHA1 instead of full script body) but
            # may fail with NOSCRIPT if Redis was restarted or script was flushed.
            try:
                result = await self._redis.evalsha(
                    self._dedupe_script_sha,  # type: ignore[attr-defined]
                    1,  # number of keys
                    dedupe_key,
                    self.IDEMPOTENCY_TTL_SEC,  # TTL argument
                )
            except Exception:  # noqa: BLE001
                # Fallback to full EVAL on NOSCRIPT or other script-related errors.
                log.debug("evalsha failed, falling back to eval")
                result = await self._redis.eval(
                    DEDUPE_SCRIPT,
                    1,  # number of keys
                    dedupe_key,
                    self.IDEMPOTENCY_TTL_SEC,  # TTL argument
                )  # type: ignore[attr-defined]

            # If result is 0, key already exists (duplicate event)
            if result == 0:
                log.info(
                    "duplicate event detected, dropping",
                    idempotency_key=event.mandalaidempotencykey,
                    event_type=event.type,
                    stream=stream,
                )
                # Return empty string to indicate event was dropped
                return ""

        # Publish to stream
        s = get_settings()
        msg_id: str = await self._redis.xadd(  # type: ignore[attr-defined]
            stream, {"e": event.to_json()}, maxlen=s.stream_maxlen, approximate=True
        )

        log.debug(
            "event published to stream",
            stream=stream,
            msg_id=msg_id,
            event_type=event.type,
            idempotency_key=event.mandalaidempotencykey,
        )

        # Dual-write to Iceberg if configured (fire-and-forget, non-blocking)
        if self._event_log:
            asyncio.create_task(self._append_to_event_log(event))

        return msg_id

    async def _append_to_event_log(self, event: MandalaEvent) -> None:
        """Append event to Iceberg event log (background task)."""
        try:
            await self._event_log.append(event)
            log.debug("event appended to iceberg log", event_id=event.id)
        except Exception:
            log.exception("iceberg append failed", event_id=event.id, event_type=event.type)
            # Iceberg failures don't block the event pipeline

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

    async def consume(
        self,
        stream: str,
        *,
        group: str,
        consumer: str,
        count: int = 32,
        block_ms: int = 5000,
    ) -> list[tuple[str, MandalaEvent]]:
        """One-shot batched consume.

        Wraps a single ``XREADGROUP`` call. Returns up to ``count`` decoded
        ``(message_id, event)`` tuples; malformed messages are auto-acked and
        skipped so they don't poison the consumer group.
        """
        await self._ensure_group(stream, group)
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
            return []

        if not resp:
            return []

        out: list[tuple[str, MandalaEvent]] = []
        for _stream_name, messages in resp:
            for msg_id, fields in messages:
                msg_id_s = msg_id if isinstance(msg_id, str) else msg_id.decode()
                raw = fields.get(b"e") if isinstance(fields, dict) else fields.get("e")
                if raw is None:
                    await self.ack(stream, group, msg_id_s)
                    continue
                try:
                    event = MandalaEvent.from_json(raw)
                except Exception:
                    log.exception("malformed event on stream", stream=stream, msg_id=msg_id_s)
                    await self.ack(stream, group, msg_id_s)
                    continue
                out.append((msg_id_s, event))
        return out

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(stream, group, message_id)  # type: ignore[attr-defined]
