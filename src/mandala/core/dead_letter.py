"""Dead letter queue for failed events.

Failed events (detector errors, projection errors, webhook validation errors)
are written to a separate Redis stream for later inspection and replay.

This prevents data loss and enables debugging of production issues.

Includes exponential backoff retry mechanism for transient failures.
"""
from __future__ import annotations

import json
import random
from datetime import UTC, datetime
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.settings import get_settings

log = structlog.get_logger(__name__)


class DeadLetterQueue:
    """Redis Streams-based dead letter queue for failed events."""

    def __init__(self, redis: object) -> None:
        self._redis = redis
        self._stream = "mandala:dlq"
        self._maxlen = 10_000  # Keep last 10k failed events
        self._retry_stream = "mandala:dlq:retry"  # Separate stream for retry scheduling

    async def publish(
        self,
        event: MandalaEvent | dict[str, Any],
        error: str,
        context: str,
        metadata: dict[str, Any] | None = None,
        retryable: bool = False,
    ) -> None:
        """Publish a failed event to the dead letter queue.

        Args:
            event: The failed event (MandalaEvent or dict)
            error: Error message or exception
            context: Context where failure occurred (e.g., "detector", "projection", "webhook")
            metadata: Additional metadata (e.g., detector name, webhook source)
            retryable: Whether this failure is retryable (for exponential backoff)
        """
        from mandala.core.events.envelope import SCHEMA_VERSION

        # Convert event to dict if needed
        event_dict = event.to_dict() if isinstance(event, MandalaEvent) else event

        dlq_entry = {
            "failed_at": datetime.now(UTC).isoformat(),
            "event": event_dict,
            "error": error,
            "context": context,
            "metadata": metadata or {},
            "schema_version": SCHEMA_VERSION,
            "retryable": retryable,
            "retry_count": 0,
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
                retryable=retryable,
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
    ) -> list[dict[str, Any]]:
        """Read failed events from the dead letter queue.

        Args:
            count: Maximum number of entries to read

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

    def _calculate_backoff(self, retry_count: int, base_delay: float = 1.0, max_delay: float = 300.0) -> float:
        """Calculate exponential backoff delay with jitter.
        
        Args:
            retry_count: Number of retry attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            
        Returns:
            Delay in seconds with jitter added
        """
        # Exponential backoff: base_delay * 2^retry_count
        delay = min(base_delay * (2 ** retry_count), max_delay)
        # Add jitter: +/- 20% random variation
        jitter = delay * 0.2 * (random.random() * 2 - 1)
        return max(delay + jitter, base_delay)

    async def schedule_retry(
        self,
        msg_id: str,
        retry_delay: float | None = None,
    ) -> bool:
        """Schedule a retry for a failed event with exponential backoff.
        
        Args:
            msg_id: Message ID from the dead letter queue
            retry_delay: Optional custom retry delay (uses exponential backoff if None)
            
        Returns:
            True if scheduled successfully, False otherwise
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
                log.warning("dead_letter.retry.not_found", msg_id=msg_id)
                return False
            
            _, fields = resp[0]
            raw = fields.get(b"entry") if isinstance(fields, dict) else fields.get("entry")
            if isinstance(raw, bytes):
                raw = raw.decode()
            entry = json.loads(raw)
            
            # Check if retryable
            if not entry.get("retryable", False):
                log.warning("dead_letter.retry.not_retryable", msg_id=msg_id)
                return False
            
            # Increment retry count
            retry_count = entry.get("retry_count", 0) + 1
            entry["retry_count"] = retry_count
            
            # Calculate retry delay
            if retry_delay is None:
                retry_delay = self._calculate_backoff(retry_count)
            
            # Calculate retry timestamp
            retry_at = datetime.now(UTC).timestamp() + retry_delay
            
            # Schedule retry in retry stream (score = retry timestamp)
            await self._redis.zadd(  # type: ignore[attr-defined]
                self._retry_stream,
                {msg_id: retry_at},
            )
            
            # Update entry with retry info
            entry["retry_scheduled_at"] = datetime.now(UTC).isoformat()
            entry["retry_at"] = datetime.fromtimestamp(retry_at, UTC).isoformat()
            
            # Update original entry
            await self._redis.xdel(self._stream, msg_id)  # type: ignore[attr-defined]
            await self._redis.xadd(  # type: ignore[attr-defined]
                self._stream,
                {"entry": json.dumps(entry, default=str)},
                maxlen=self._maxlen,
                approximate=True,
            )
            
            log.info(
                "dead_letter.retry_scheduled",
                msg_id=msg_id,
                retry_count=retry_count,
                retry_delay_sec=retry_delay,
                retry_at=datetime.fromtimestamp(retry_at, UTC).isoformat(),
            )
            
            return True
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.retry_schedule_failed", msg_id=msg_id, error=str(exc))
            return False

    async def process_retries(self) -> int:
        """Process due retries from the retry stream.
        
        Returns:
            Number of retries processed
        """
        try:
            now = datetime.now(UTC).timestamp()
            
            # Get all due retries (score <= now)
            due_retries = await self._redis.zrangebyscore(  # type: ignore[attr-defined]
                self._retry_stream,
                "-inf",
                now,
            )
            
            processed = 0
            for msg_id in due_retries:
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode()
                
                # Replay the event
                if await self.replay(msg_id):
                    # Remove from retry stream
                    await self._redis.zrem(self._retry_stream, msg_id)  # type: ignore[attr-defined]
                    processed += 1
                else:
                    # Replay failed, reschedule with exponential backoff
                    await self.schedule_retry(msg_id)
            
            if processed > 0:
                log.info("dead_letter.retries_processed", count=processed)
            
            return processed
        except Exception as exc:  # noqa: BLE001
            log.exception("dead_letter.retry_process_failed", error=str(exc))
            return 0
