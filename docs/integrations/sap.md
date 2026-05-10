# SAP Integration Pattern

**Version:** 1.0
**Date:** 2026-05-10

## Overview

SAP Transportation Management (SAP TM) and SAP Extended Warehouse Management (SAP EWM) integration for real-time logistics telemetry.

**Telemetry in:** Samsara truck location, Descartes customs status → SAP TM/EWM
**Telemetry out:** SAP TM shipment changes, SAP EWM inventory changes → Mandala events

## Why SAP Integration Matters

**SAP's bottleneck:**
- SAP has ERP data (orders, shipments, invoices)
- SAP lacks real-time operational data (fleet telemetry, customs status, border crossings)
- SAP TM needs real-time truck location and customs status for shipment tracking
- SAP EWM needs real-time truck arrival for yard scheduling

**Mandala's hook:**
- Mandala has real-time fleet telemetry (Samsara)
- Mandala has real-time customs status (Descartes MacroPoint)
- Mandala has real-time border crossing data
- Mandala can push this data to SAP TM/EWM
- Mandala can ingest SAP TM/EWM changes as Mandala events

## Architecture

```
Telemetry In (Mandala → SAP):
Samsara Truck Location → Mandala → SAP TM Shipment Tracking
Descartes Customs Status → Mandala → SAP TM Shipment Compliance
Mandala Border Crossing → Mandala → SAP TM Shipment Milestone

Telemetry Out (SAP → Mandala):
SAP TM Shipment Change → Mandala → Mandala Event
SAP EWM Inventory Change → Mandala → Mandala Event
SAP HANA CDC → Mandala → Mandala Event
```

## Configuration

```bash
# .env
MANDALA_SAP_ENABLED=1
MANDALA_SAP_HOST=sap-system.example.com
MANDALA_SAP_PORT=44300
MANDALA_SAP_CLIENT_ID=your-sap-oauth-client-id
MANDALA_SAP_CLIENT_SECRET=your-sap-oauth-client-secret
```

## Telemetry In (Mandala → SAP)

**Pattern:**
```python
# src/mandala/connectors/sap/connector.py
async def push_to_sap(self, event: MandalaEvent) -> bool:
    """Push MandalaEvent to SAP TM/EWM."""
    if not self.is_configured():
        return False

    async with httpx.AsyncClient() as client:
        # SAP TM API call to update shipment status
        # SAP EWM API call to update yard scheduling
        pass
```

**Event Types:**
- `mandala.truck.location.updated` → SAP TM shipment tracking
- `mandala.customs.filing.landed` → SAP TM shipment compliance
- `mandala.border.crossing` → SAP TM shipment milestone
- `mandala.truck.geofence.entered` → SAP EWM yard scheduling

**SAP TM Integration:**
- Update SAP TM shipment with real-time truck location
- Update SAP TM shipment with customs status
- Update SAP TM shipment with border crossing milestones
- SAP TM API: `/sap/opu/odata/sap/API_TM_FO_SHIPMENT`

**SAP EWM Integration:**
- Update SAP EWM yard with truck arrival
- Update SAP EWM dock door assignment
- SAP EWM API: `/sap/opu/odata/sap/API_EWM_FO_INBOUND_DELIVERY`

## Telemetry Out (SAP → Mandala)

**Pattern:**
```python
# Use existing CDC infrastructure (src/mandala/core/cdc.py)
# SAP HANA CDC can be implemented following the PostgresCDC pattern
from mandala.core.cdc import CDCConsumer

class SAPHanaCDC(CDCConsumer):
    """SAP HANA CDC consumer."""
    async def _consume(self) -> None:
        # Monitor SAP TM/EWM tables
        # Emit MandalaEvents for changes
        pass
```

**Event Types:**
- SAP TM shipment created → `mandala.sap.shipment.created`
- SAP TM shipment leg updated → `mandala.sap.shipment.leg.updated`
- SAP EWM inventory change → `mandala.sap.inventory.updated`
- SAP EWM yard assignment → `mandala.sap.yard.assigned`

**SAP TM Tables:**
- `/TMF/SHIPMENT` - Shipments
- `/TMF/SHIPMENT_LEG` - Shipment legs
- `/TMF/FREIGHT_UNIT` - Freight units

**SAP EWM Tables:**
- `/EWM/INBOUND_DELIVERY` - Inbound deliveries
- `/EWM/OUTBOUND_DELIVERY` - Outbound deliveries
- `/EWM/WAREHOUSE_TASK` - Warehouse tasks

## Implementation Notes

**Existing Infrastructure Used:**
- `src/mandala/core/connector.py` - Base Connector class
- `src/mandala/core/cdc.py` - CDC infrastructure (PostgresCDC, MySQLCDC)
- `src/mandala/core/schema/identifiers.py` - URN identifier system
- `httpx` - HTTP client (same as Samsara, Descartes connectors)

**New Code:**
- `src/mandala/connectors/sap/__init__.py` - SAP connector package
- `src/mandala/connectors/sap/connector.py` - SAP connector stub
- `docs/integrations/sap.md` - This documentation

**Stub Status:**
- SAP connector is a stub (not full implementation)
- Uses existing Connector base class
- Uses existing httpx client pattern
- Can be extended for full SAP TM/EWM integration

## Why This Matters

**Strategic positioning:**
- SAP TM is the logistics execution layer
- SAP TM needs real-time truck location and customs status
- Mandala has the real-time execution data
- Mandala is the event producer for SAP TM/EWM

**Acquisition angle:**
- SAP acquires event-driven infrastructure to feed SAP Event Mesh
- Mandala is the event producer for logistics
- SAP TM integration makes Mandala enterprise-grade
- SAP + Mandala = real-time logistics data feeding SAP TM/EWM

## References

- SAP TM API: https://api.sap.com/api/API_TM_FO_SHIPMENT
- SAP EWM API: https://api.sap.com/api/API_EWM_FO_INBOUND_DELIVERY
- SAP Event Mesh: https://help.sap.com/docs/event-mesh
- SAP HANA CDC: https://help.sap.com/docs/hana

## Summary

This integration pattern enables bidirectional telemetry between Mandala and SAP TM/EWM:
- Telemetry in: Samsara truck location, Descartes customs status → SAP TM/EWM
- Telemetry out: SAP TM/EWM changes → Mandala events
- Uses existing Connector base class and CDC infrastructure
- Stub implementation that can be extended for full SAP TM/EWM integration

**Total effort:** ~100 lines (connector stub + documentation)
**Value:** SAP TM/EWM integration hook
**Timeline:** 1-2 hours (stub), 4-6 weeks (full implementation)
