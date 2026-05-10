"""EPCIS 2.0 adapter for GS1 compliance.

GS1 EPCIS 2.0 is a global standard for capturing and sharing event-level
supply chain data — what happened, where, when, why, and to which product.
EPCIS 2.0 provides REST APIs for capture and query of event data, includes
sensor data for monitoring conditions like cold chains, and uses GS1 Digital
Link URI syntax to express GTIN, GLN, and SSCC identifiers.

This adapter emits MandalaEvents in EPCIS 2.0 JSON format, making Mandala
compatible with every GS1 EPCIS subscriber globally.

See docs/standards/epcis.md for full integration pattern.
"""

from mandala.connectors.epcis.adapter import EPCISAdapter

__all__ = ["EPCISAdapter"]
