"""Runner process for materialized views.

Subscribes to the same inbound stream as the worker but with a **separate
consumer group** (``mandala:views``) so that slow or failing views can
never back up the detector pipeline. Events are fanned out to every
enabled view via :func:`asyncio.gather` with ``return_exceptions=True``;
one view crashing does not block the others.

Run with::

    mandala views
"""
from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone

import redis.asyncio as redis
import structlog

from mandala.core.bus import RedisStreamsBus
from mandala.core.metrics import (
    start_metrics_server,
    view_apply_duration_seconds,
    view_apply_total,
)
from mandala.core.state import StateStore
from mandala.settings import get_settings
from mandala.views.base import MaterializedView
from mandala.views.bitmap import BitmapView
from mandala.views.geospatial import GeospatialView
from mandala.views.graph import GraphView
from mandala.views.timeseries import TimeseriesView

log = structlog.get_logger(__name__)


def _build_views(r: "object") -> list[MaterializedView]:
    s = get_settings()
    views: list[MaterializedView] = []
    if s.views_geospatial_enabled:
        views.append(GeospatialView(r))
    if s.views_timeseries_enabled:
        views.append(TimeseriesView(r))
    if s.views_bitmap_enabled:
        views.append(BitmapView(r, StateStore(r)))
    if s.views_graph_enabled:
        views.append(GraphView(r))
    return views


async def run() -> None:
    s = get_settings()
    r = redis.from_url(s.redis_url, decode_responses=False)
    bus = RedisStreamsBus(r)
    views = _build_views(r)
    consumer = f"{socket.gethostname()}-{os.getpid()}"

    if not views:
        log.warning("mandala.views.no_views_enabled")
        await r.aclose()
        return

    if s.metrics_enabled:
        start_metrics_server(s.metrics_port)

    log.info(
        "mandala.views.start",
        stream=s.stream_inbound,
        group=s.views_consumer_group,
        consumer=consumer,
        views=[v.name for v in views],
    )

    try:
        while True:
            messages = await bus.consume(
                s.stream_inbound,
                group=s.views_consumer_group,
                consumer=consumer,
                count=32,
                block_ms=5000,
            )
            if not messages:
                continue

            for msg_id, event in messages:
                # Fan out to all views in parallel; a single view crashing
                # must never block or drop the event for other views.
                start = datetime.now(timezone.utc)
                results = await asyncio.gather(
                    *(v.apply(event) for v in views),
                    return_exceptions=True,
                )
                duration = (datetime.now(timezone.utc) - start).total_seconds()

                for view, result in zip(views, results, strict=True):
                    status = "success"
                    if isinstance(result, BaseException):
                        status = "failure"
                        log.exception(
                            "mandala.views.apply_failed",
                            view=view.name,
                            event_type=event.type,
                            event_id=event.id,
                            exc_info=result,
                        )
                    view_apply_total.labels(view=view.name, status=status).inc()
                    view_apply_duration_seconds.labels(view=view.name).observe(duration)

                # Always ack — views are best-effort projections, and a
                # stuck event here should not back up the stream.
                await bus.ack(s.stream_inbound, s.views_consumer_group, msg_id)
    finally:
        await r.aclose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
