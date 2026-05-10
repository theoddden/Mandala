"""SAP connector for logistics integration.

SAP Transportation Management (SAP TM) and SAP Extended Warehouse Management (SAP EWM)
integration for real-time logistics telemetry.

Telemetry in: Samsara truck location, Descartes customs status → SAP TM/EWM
Telemetry out: SAP TM shipment changes, SAP EWM inventory changes → Mandala events

See docs/integrations/sap.md for integration pattern.
"""

from mandala.connectors.sap.connector import SAPConnector

__all__ = ["SAPConnector"]
