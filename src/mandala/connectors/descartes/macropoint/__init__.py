"""Descartes MacroPoint — freight-visibility connector.

Scope (v1, public carrier docs only):
* Inbound: receive Tracking Requests from MacroPoint
  (carrier-side webhook) and ack them.
* Outbound: send Location Updates back to MacroPoint when the
  paired Samsara vehicle reports a new position.
* Bidirectional: when Samsara loses signal on a vehicle, fall back
  to MacroPoint carrier-reported position; when MacroPoint receives
  a carrier ETA absent from Samsara, merge it into the canonical
  shipment.

This works without any commercial Descartes subscription — MacroPoint
publishes the integration spec for carriers using the platform.
"""

from mandala.connectors.descartes.macropoint.connector import MacroPointConnector

__all__ = ["MacroPointConnector"]
