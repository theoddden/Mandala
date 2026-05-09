"""FMCSA SAFER API connector for carrier safety profile enrichment.

The FMCSA (Federal Motor Carrier Safety Administration) SAFER API is a free,
public API that provides carrier safety data including CSA scores across all
seven BASIC categories, inspection history, violation records, out-of-service
rate, and operating authority status.

This connector is used for enrichment only — it does not receive webhooks.
Instead, it is called from the worker pipeline when a DOT number is present
on a carrier event to enrich the carrier object with live FMCSA data.
"""
from __future__ import annotations

from mandala.connectors.fmcsa.client import FMCSAClient
from mandala.connectors.fmcsa.connector import FMCSAConnector

__all__ = ["FMCSAClient", "FMCSAConnector"]
