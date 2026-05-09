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
@click.option("--root", default="./warehouse", show_default=True, help="Output directory for JSONL files.")
def sink(root: str) -> None:
    """Run the warehouse sink — writes raw_mandala_events JSONL files for dbt-mandala."""
    from mandala.sinks.runner import run as sink_run
    import asyncio

    asyncio.run(sink_run(root))


@main.command()
def schema() -> None:
    """Print the canonical Mandala schema version."""
    from mandala.core.events.envelope import SCHEMA_VERSION

    click.echo(SCHEMA_VERSION)


if __name__ == "__main__":
    main()
