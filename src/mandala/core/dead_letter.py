"""Dead letter queue for failed events.

Failed events (detector errors, projection errors, webhook validation errors)
are written to a separate Redis stream for later inspection and replay.

This prevents data loss and enables debugging of production issues.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class DeadLetterQueue:
    """Redis Streams-based dead letter queue for failed events."""

    def __init__(self, redis: "object") -> None:
        self._redis = redis
        self._stream = "mandala:dlq"
        self._maxlen = 10_000  # Keep last 10k failed events

    async def publish(
        self,
        event: MandalaEvent | dict[str, Any],
        error: str,
        context: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Publish a failed event to the dead letter queue.

        Args:
            event: The failed event (MandalaEvent or dict)
            error: Error message or exception
            context: Context where failure occurred (e.g., "detector", "projection", "webhook")
            metadata: Additional metadata (e.g., detector name, webhook source)
        """
        s = get_settings()
        
        # Convert event to dict if needed
        event_dict = event.to_dict() if isinstance(event, MandalaEvent) else event
        
        dlq_entry = {
            "failed_at": datetime.now(UTC).isoformat(),
            "event": event_dict,
            "error": error,
            "context": context,
            "metadata": metadata or {},
            "schema_version": s.state_ttl_seconds,  # Use settings as proxy for version
        }

        try:
            await self._redis.xadd(  # type: ignore[attr-defined]
                self._stream,
                {"entry": json.dumps(dlq_entry, default=str)},
                maxlen=self._maxlen,
                approximate=True,
            )
            log.warning(
                "dead_letter.published",
                context=context,
                error=error,
                event_id=event_dict.get("id"),
                event_type=event_dict.get("type"),
            )
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "dead_letter.publish_failed",
                context=context,
                error=str(exc),
            )

    async def read(
        self,
        count: int = 100,
        block_ms: int = 5000,
    ) -> list[dict[str, Any]]:
        """Read failed events from the dead letter queue.

        Args:
            count: Maximum number of entries to read
            block_ms: Block time in milliseconds

        Returns:
            List of dead letter queue entries
        """
        try:
            resp = await self._redis.xrevrange(  # type: ignore[attr-defined]
                self._stream,
                "+",
                "-",
                count=count,
            )
            
            entries = []
            for msg_id, fields in resp:
                raw = fields.get(b"entry") if isinstance(fields, dict) else fields.get("entry")
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    entry = json.loads(raw)
                    entry["msg_id"] = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    entries.append(entry)
            
            return entries
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.read_failed", error=str(exc))
            return []

    async def replay(self, msg_id: str) -> bool:
        """Replay a failed event back to the main stream.

        Args:
            msg_id: Message ID from the dead letter queue

        Returns:
            True if replayed successfully, False otherwise
        """
        try:
            # Read the specific entry
            resp = await self._redis.xrange(  # type: ignore[attr-defined]
                self._stream,
                msg_id,
                msg_id,
                count=1,
            )
            
            if not resp:
                log.warning("dead_letter.replay.not_found", msg_id=msg_id)
                return False
            
            _, fields = resp[0]
            raw = fields.get(b"entry") if isinstance(fields, dict) else fields.get("entry")
            if isinstance(raw, bytes):
                raw = raw.decode()
            entry = json.loads(raw)
            
            # Republish to main stream
            from mandala.core.events.envelope import MandalaEvent
            event = MandalaEvent.model_validate(entry["event"])
            
            s = get_settings()
            from mandala.core.bus import RedisStreamsBus
            bus = RedisStreamsBus(self._redis)
            
            await bus.publish(s.stream_inbound, event, enable_deduplication=False)
            
            # Remove from DLQ
            await self._redis.xdel(self._stream, msg_id)  # type: ignore[attr-defined]
            
            log.info(
                "dead_letter.replayed",
                msg_id=msg_id,
                event_id=event.id,
                event_type=event.type,
            )
            
            return True
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.replay_failed", msg_id=msg_id, error=str(exc))
            return False

    async def delete(self, msg_id: str) -> bool:
        """Delete a failed event from the dead letter queue.

        Args:
            msg_id: Message ID to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            await self._redis.xdel(self._stream, msg_id)  # type: ignore[attr-defined]
            log.info("dead_letter.deleted", msg_id=msg_id)
            return True
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.delete_failed", msg_id=msg_id, error=str(exc))
            return False

    async def stats(self) -> dict[str, Any]:
        """Get dead letter queue statistics.

        Returns:
            Dictionary with DLQ stats (length, oldest entry, etc.)
        """
        try:
            length = await self._redis.xlen(self._stream)  # type: ignore[attr-defined]
            
            # Get oldest entry
            oldest = None
            resp = await self._redis.xrange(  # type: ignore[attr-defined]
                self._stream,
                "-",
                "+",
                count=1,
            )
            if resp:
                _, fields = resp[0]
                raw = fields.get(b"entry") if isinstance(fields, dict) else fields.get("entry")
                if isinstance(raw, bytes):
                    raw = raw.decode()
                entry = json.loads(raw)
                oldest = entry.get("failed_at")
            
            return {
                "length": length,
                "maxlen": self._maxlen,
                "oldest_entry": oldest,
                "utilization": length / self._maxlen if self._maxlen else 0,
            }
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.stats_failed", error=str(exc))
            return {"error": str(exc)}
