"""Mandala CLI.

Three commands; that's all you need:

    mandala serve     # FastAPI webhook ingest (uvicorn)
    mandala worker    # event loop: project + alert
    mandala mcp       # MCP stdio server for LLMs
"""
from __future__ import annotations

import click

from mandala import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="mandala")
def main() -> None:
    """Mandala — the bridge between the wheel and the plane."""


@main.command()
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
@click.option("--reload", is_flag=True, help="Auto-reload on code changes (dev).")
def serve(host: str, port: int, reload: bool) -> None:
    """Run the webhook ingest API."""
    import uvicorn

    uvicorn.run("mandala.app:app", host=host, port=port, reload=reload, factory=False)


@main.command()
def worker() -> None:
    """Run the projection + alert worker."""
    from mandala.worker import main as worker_main

    worker_main()


@main.command()
def mcp() -> None:
    """Run the MCP stdio server."""
    from mandala.mcp import main as mcp_main

    mcp_main()


@main.command()
@click.option("--rebuild", is_flag=True, help="Delete and rebuild all materialized views before starting.")
def views(rebuild: bool) -> None:
    """Run the materialized-views runner (geospatial / timeseries / bitmap / graph)."""
    from mandala.views.runner import main as views_main

    views_main(rebuild=rebuild)


@main.command()
@click.option("--root", default="./warehouse", show_default=True, help="Output directory for JSONL files.")
def sink(root: str) -> None:
    """Run the warehouse sink — writes raw_mandala_events JSONL files for dbt-mandala."""
    import asyncio

    from mandala.sinks.runner import run as sink_run

    asyncio.run(sink_run(root))


@main.command()
def schema() -> None:
    """Print the canonical Mandala schema version."""
    from mandala.core.events.envelope import SCHEMA_VERSION

    click.echo(SCHEMA_VERSION)


@main.command()
@click.option("--from", "from_dt", required=True, help="Start datetime (ISO format, e.g., 2026-04-01T00:00:00Z)")
@click.option("--to", "to_dt", required=True, help="End datetime (ISO format, e.g., 2026-04-15T23:59:59Z)")
@click.option("--entity", help="Replay events for a specific entity URN only")
@click.option("--detector", help="Filter by detector name (not yet implemented)")
@click.option("--dry-run", is_flag=True, help="Don't write to state, just show what would happen")
@click.option("--stream", is_flag=True, help="Replay from Redis Stream instead of Iceberg (for recent events)")
@click.option("--count", default=1000, help="Number of events to replay from stream (only with --stream)")
def replay(from_dt: str, to_dt: str, entity: str | None, detector: str | None, dry_run: bool, stream: bool, count: int) -> None:
    """Replay historical events to fix state after bugs."""
    import asyncio
    from datetime import datetime

    import redis.asyncio as redis

    from mandala.core.replay import EventReplay, replay_from_stream
    from mandala.core.state import StateStore
    from mandala.settings import get_settings

    async def run_replay() -> None:
        s = get_settings()
        r = redis.from_url(s.redis_url, decode_responses=False)
        state = StateStore(r)

        if stream:
            # Replay from Redis Stream (recent events)
            click.echo(f"Replaying last {count} events from Redis Stream...")
            stats = await replay_from_stream(r, state, s.stream_inbound, count, dry_run)
        else:
            # Replay from Iceberg (historical events)
            from mandala.core.event_log import EventLog

            if not s.event_log_enabled:
                click.echo("Error: Iceberg event log not configured. Set MANDALA_EVENT_LOG_ENABLED=1")
                return

            event_log = EventLog()
            replay = EventReplay(event_log, state)

            if entity:
                click.echo(f"Replaying events for entity: {entity}")
                stats = await replay.replay_entity(
                    entity_urn=entity,
                    from_dt=datetime.fromisoformat(from_dt),
                    to_dt=datetime.fromisoformat(to_dt),
                    dry_run=dry_run,
                )
            else:
                click.echo(f"Replaying events from {from_dt} to {to_dt}")
                stats = await replay.replay_range(
                    from_dt=datetime.fromisoformat(from_dt),
                    to_dt=datetime.fromisoformat(to_dt),
                    detector_filter=detector,
                    dry_run=dry_run,
                )

        await r.aclose()
        click.echo(f"Replay complete: {stats}")

    asyncio.run(run_replay())


if __name__ == "__main__":
    main()
