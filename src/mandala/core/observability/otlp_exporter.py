"""OTLP span exporter for Mandala events.

Sends ``MandalaEvent`` instances as OpenTelemetry spans over OTLP/HTTP to a
collector endpoint (e.g. ``http://otel-collector:4318/v1/traces``).

Design goals:

* **Zero overhead when disabled.** If ``MANDALA_OTLP_ENDPOINT`` is empty the
  exporter is a no-op singleton; no HTTP client, no background task, nothing.
* **Lazy dependency.** No ``opentelemetry-*`` package required — we speak
  OTLP/HTTP-JSON directly with ``httpx``, which Mandala already uses.
* **Batched & non-blocking.** Spans are queued in memory and flushed every
  ``flush_interval_sec`` (default 2s) on a background task, so the worker
  hot path stays sub-millisecond.
* **Drop-on-overflow.** If the queue exceeds ``max_queue_size`` (default
  10_000) we drop oldest spans and log once per flush — observability must
  never back-pressure the bus.
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from typing import Any

import httpx

from mandala.core.events.envelope import MandalaEvent

log = logging.getLogger(__name__)

_DEFAULT_FLUSH_INTERVAL_SEC = 2.0
_DEFAULT_MAX_QUEUE_SIZE = 10_000
_DEFAULT_BATCH_SIZE = 256

_SERVICE_NAME_KEY = "service.name"
_SERVICE_VERSION_KEY = "service.version"


class OTLPExporter:
    """Batched OTLP/HTTP-JSON span exporter.

    Instantiate once per process via :func:`get_exporter`. The exporter
    accepts events synchronously via :meth:`emit` and flushes them on a
    background asyncio task.

    When ``endpoint`` is falsy the exporter is a no-op (zero overhead).
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        service_name: str = "mandala",
        service_version: str = "0.3",
        flush_interval_sec: float = _DEFAULT_FLUSH_INTERVAL_SEC,
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.endpoint = endpoint or ""
        self.service_name = service_name
        self.service_version = service_version
        self.flush_interval_sec = flush_interval_sec
        self.max_queue_size = max_queue_size
        self.batch_size = batch_size
        self.headers = headers or {}

        self._queue: deque[dict[str, Any]] = deque(maxlen=max_queue_size)
        self._dropped_since_last_flush = 0
        self._client: httpx.AsyncClient | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    def emit(self, event: MandalaEvent) -> None:
        """Queue an event for OTLP export. No-op when disabled."""
        if not self.enabled:
            return
        if len(self._queue) == self.max_queue_size:
            self._dropped_since_last_flush += 1
        self._queue.append(event.to_otlp_span())

    async def start(self) -> None:
        """Start the background flush task. Idempotent and a no-op when disabled."""
        if not self.enabled or self._task is not None:
            return
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(5.0))
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="mandala-otlp-exporter")
        log.info("mandala.otlp.exporter.started endpoint=%s", self.endpoint)

    async def stop(self) -> None:
        """Flush remaining spans and stop the background task."""
        if self._task is None:
            return
        self._stopped.set()
        try:
            await self._task
        finally:
            self._task = None
            await self._flush()  # final drain
            if self._client is not None:
                await self._client.aclose()
                self._client = None
        log.info("mandala.otlp.exporter.stopped")

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.flush_interval_sec
                )
            except TimeoutError:
                pass
            await self._flush()

    async def _flush(self) -> None:
        if not self._queue or self._client is None:
            return
        # Snapshot up to batch_size spans
        spans: list[dict[str, Any]] = []
        while self._queue and len(spans) < self.batch_size:
            spans.append(self._queue.popleft())
        if not spans:
            return

        payload = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": _SERVICE_NAME_KEY, "value": {"stringValue": self.service_name}},
                            {"key": _SERVICE_VERSION_KEY, "value": {"stringValue": self.service_version}},
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "mandala", "version": self.service_version},
                            "spans": spans,
                        }
                    ],
                }
            ]
        }
        try:
            resp = await self._client.post(
                self.endpoint,
                json=payload,
                headers={"Content-Type": "application/json", **self.headers},
            )
            if resp.status_code >= 400:
                log.warning(
                    "mandala.otlp.export.failed status=%s body=%s",
                    resp.status_code,
                    resp.text[:200],
                )
        except Exception as e:  # noqa: BLE001 - we must not break the worker
            log.warning("mandala.otlp.export.exception %s", e)

        if self._dropped_since_last_flush:
            log.warning(
                "mandala.otlp.queue.dropped count=%s", self._dropped_since_last_flush
            )
            self._dropped_since_last_flush = 0


# --- Singleton -------------------------------------------------------------

_exporter: OTLPExporter | None = None


def get_exporter() -> OTLPExporter:
    """Return the process-wide OTLP exporter (no-op if disabled).

    Reads ``MANDALA_OTLP_ENDPOINT`` from the environment. To override, use
    :class:`mandala.settings.Settings` and construct an :class:`OTLPExporter`
    directly.
    """
    global _exporter
    if _exporter is None:
        endpoint = os.getenv("MANDALA_OTLP_ENDPOINT", "").strip()
        _exporter = OTLPExporter(endpoint=endpoint)
    return _exporter
