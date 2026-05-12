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
from datetime import UTC, datetime

import redis.asyncio as redis
import structlog

from mandala.core.bus import RedisStreamsBus
from mandala.core.metrics import (
    consumer_group_lag,
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


async def _probe_redis_version(redis: object) -> str:
    """Probe Redis server version at startup for feature detection."""
    try:
        info = await redis.info("server")  # type: ignore[attr-defined]
        # redis-py returns dict; version is in info['redis_version']
        version = info.get("redis_version", "unknown") if isinstance(info, dict) else "unknown"
        log.info("redis_version_probe", version=version)
        return str(version)
    except Exception as exc:  # noqa: BLE001
        log.exception("redis_version_probe_failed", error=str(exc))
        return "unknown"


async def _publish_consumer_group_lag(
    redis: object, stream: str, group: str, interval_sec: float = 10.0
) -> None:
    """Background task to publish consumer group lag metrics."""
    while True:
        try:
            # XINFO GROUPS returns info about consumer groups for a stream
            info = await redis.xinfo_groups(stream)  # type: ignore[attr-defined]
            if isinstance(info, list):
                for group_info in info:
                    if isinstance(group_info, dict):
                        group_name = group_info.get("name")
                        if group_name == group:
                            lag = group_info.get("lag", 0)
                            consumer_group_lag.labels(stream=stream, group=group).set(lag)
                            break
        except Exception as exc:  # noqa: BLE001
            log.exception("consumer_group_lag_publish_failed", error=str(exc))
        await asyncio.sleep(interval_sec)


def _build_views(r: object) -> list[MaterializedView]:
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


async def _rebuild_views(r: object, views: list[MaterializedView]) -> None:
    """Delete all view keys from Redis to trigger a full rebuild."""
    log.info("mandala.views.rebuild_start", views=[v.name for v in views])
    # Pattern for all view keys
    patterns = [
        "mandala:view:gs:*",  # geospatial
        "mandala:view:ts:*",  # timeseries
        "mandala:view:bm:*",  # bitmap
        "mandala:view:graph:*",  # graph
    ]
    for pattern in patterns:
        try:
            keys = await r.keys(pattern)  # type: ignore[attr-defined]
            if keys:
                await r.delete(*keys)  # type: ignore[attr-defined]
                log.info("mandala.views.rebuild_deleted", pattern=pattern, count=len(keys))
        except Exception as exc:  # noqa: BLE001
            log.exception("mandala.views.rebuild_failed", pattern=pattern, error=str(exc))
    log.info("mandala.views.rebuild_complete")


async def run(rebuild: bool = False) -> None:
    s = get_settings()
    r = redis.from_url(s.redis_url, decode_responses=False)

    # Probe Redis version at startup for feature detection
    await _probe_redis_version(r)

    bus = RedisStreamsBus(r)
    views = _build_views(r)
    consumer = f"{socket.gethostname()}-{os.getpid()}"

    if not views:
        log.warning("mandala.views.no_views_enabled")
        await r.aclose()
        return

    # Rebuild views if requested
    if rebuild:
        await _rebuild_views(r, views)

    if s.metrics_enabled:
        start_metrics_server(s.metrics_port)

    # Start background task to publish consumer group lag metrics
    lag_task = asyncio.create_task(
        _publish_consumer_group_lag(r, s.stream_inbound, s.views_consumer_group)
    )

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
                start = datetime.now(UTC)
                results = await asyncio.gather(
                    *(v.apply(event) for v in views),
                    return_exceptions=True,
                )
                duration = (datetime.now(UTC) - start).total_seconds()

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
        lag_task.cancel()
        await r.aclose()


def main(rebuild: bool = False) -> None:
    asyncio.run(run(rebuild=rebuild))


if __name__ == "__main__":
    main()
