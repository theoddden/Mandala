"""Samsara connector — Mandala's reference connector and Day-1 deliverable.

Mandala is fully useful with only Samsara configured: install the package,
set ``MANDALA_SAMSARA_API_TOKEN``, point a Samsara webhook at
``/webhooks/samsara``, and you'll see normalized :class:`MandalaEvent`
objects on the bus within minutes.
"""

from mandala.connectors.samsara.connector import SamsaraConnector

__all__ = ["SamsaraConnector"]
