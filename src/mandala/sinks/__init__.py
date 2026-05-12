"""Warehouse sinks: write the normalized event stream to ``raw_mandala_events``.

The sink is intentionally append-only and dumb: one row per event with
the CloudEvents envelope flattened into seven columns and the ``data``
payload stored as raw JSON. ``dbt-mandala`` does the rest.

Two sinks ship with v0.1:

* :class:`JsonlFileSink` — newline-delimited JSON files in a directory,
  rotated daily. Useful for local dev and air-gapped deploys.
* :class:`S3JsonlSink` — same format, written to S3. Pair with Snowflake
  Snowpipe / BigQuery Storage Transfer / Redshift COPY for the live
  pipeline.

To add Snowflake / BigQuery / Postgres direct sinks: subclass
:class:`Sink` and implement ``write_batch``.
"""

from mandala.sinks.base import Sink, SinkRecord
from mandala.sinks.jsonl import JsonlFileSink

__all__ = ["Sink", "SinkRecord", "JsonlFileSink"]
