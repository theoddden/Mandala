"""WiseTech CargoWise — eAdaptor connector.

CargoWise is the dominant global freight-forwarder TMS. It exposes the
**eAdaptor** integration surface, which speaks XML messages from the
Universal* schema family (Universal Shipment, Universal Event,
Universal Interchange).

Mandala v0.1 covers the high-volume cases:

* **Inbound webhook** at ``/webhooks/cargowise`` accepts Universal Event
  XML pushed from a CargoWise outbound subscription and emits
  ``mandala.shipment.*`` / ``mandala.shipment.customs.*`` events.
* **Outbound client** :class:`CargoWiseClient` posts Universal Event
  XML to a CargoWise eAdaptor inbound endpoint (typically used by
  playbooks / cross-border alerts to push a status back into CargoWise).

Auth: HTTP Basic on the eAdaptor inbound endpoint; HMAC-SHA256 over the
raw body on inbound webhooks (CargoWise calls this the "Authentication
Token" header).

References:
* WiseTech Developer Portal, eAdaptor & Universal Schema documentation.
* The XML namespace for the Universal schema is
  ``http://www.cargowise.com/Schemas/Universal/2011/11`` (the "2011/11"
  revision is current at time of writing; newer revisions are
  backwards-compatible at the field level).
"""

from mandala.connectors.cargowise.client import CargoWiseClient
from mandala.connectors.cargowise.connector import CargoWiseConnector

__all__ = ["CargoWiseClient", "CargoWiseConnector"]
