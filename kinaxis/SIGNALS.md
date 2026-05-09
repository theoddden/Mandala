# Mandala → Kinaxis Maestro Integration Signals

## Purpose

This document describes the disruption signals that Mandala emits to Kinaxis Maestro, enabling real-time supply chain planning and execution based on fleet telemetry and trade/customs events.

## Integration Architecture

```
┌─────────────┐    MandalaEvent    ┌──────────┐    Maestro Disruption    ┌─────────────┐
│  Samsara    │ ──────────────────▶ │ Mandala  │ ────────────────────────▶ │  Kinaxis    │
│  Descartes  │                     │  Bridge  │                           │  Maestro    │
└─────────────┘                     └──────────┘                           └─────────────┘
                                           │
                                           ▼
                                    Redis Streams
                                           │
                                           ▼
                              kinaxis/connector.py
                                           │
                                           ▼
                              Kinaxis Maestro API
```

## Disruption Signal Types

### 1. BORDER_DELAY

**Trigger:** `mandala.border.crossing` events

**Description:** Truck crossing a US-Mexico or US-Canada Port-of-Entry (POE). Critical when crossing occurs without matching customs filing.

**Severity Mapping:**
- `INFO` - Border crossing with customs filing present
- `CRITICAL` - Border crossing without customs filing (potential delay)

**Payload Schema:**
```json
{
  "disruption_type": "BORDER_DELAY",
  "entity_id": "truck_12345",
  "entity_type": "TRUCK",
  "severity": "CRITICAL",
  "timestamp": "2026-05-09T16:30:00Z",
  "description": "Border crossing at OTAY without customs filing - potential delay",
  "impact": {
    "portOfEntry": "OTAY",
    "customsFilingPresent": false,
    "detectionLagSeconds": 45
  },
  "source_system": "MANDALA",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **Replanning:** Trigger route recalculation for affected shipments
- **Alerting:** Notify dispatchers of trucks at POE without filings
- **KPI Tracking:** Measure POE crossing efficiency vs. filing compliance

---

### 2. COLD_CHAIN_BREACH

**Trigger:** `mandala.cold_chain.breach` events

**Description:** Temperature deviation from declared shipment range. Regulatory liability event.

**Severity:** Always `CRITICAL`

**Payload Schema:**
```json
{
  "disruption_type": "COLD_CHAIN_BREACH",
  "entity_id": "shipment_67890",
  "entity_type": "SHIPMENT",
  "severity": "CRITICAL",
  "timestamp": "2026-05-09T17:15:00Z",
  "description": "Cold chain breach: temperature 8°C outside range {'min': 2, 'max': 4}",
  "impact": {
    "temperature": 8.0,
    "declaredRange": {"min": 2, "max": 4},
    "breachWindow": {
      "start": "2026-05-09T17:00:00Z",
      "end": "2026-05-09T17:30:00Z"
    },
    "regulatoryImpact": "FDA_REPORTABLE"
  },
  "source_system": "MANDALA",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **Quality Hold:** Auto-hold affected shipments in inventory
- **Replanning:** Reroute to nearest cold storage facility
- **Regulatory Reporting:** Flag for FSMA/FDA documentation
- **Carrier Performance:** Track carrier cold chain compliance KPIs

---

### 3. CUSTOMS_HOLD

**Trigger:** `mandala.customs.hold` events

**Description:** Customs hold placed on shipment. Blocks movement until resolution.

**Severity:** Always `CRITICAL`

**Payload Schema:**
```json
{
  "disruption_type": "CUSTOMS_HOLD",
  "entity_id": "shipment_11111",
  "entity_type": "SHIPMENT",
  "severity": "CRITICAL",
  "timestamp": "2026-05-09T18:00:00Z",
  "description": "Customs hold: Documentation incomplete - missing BOL",
  "impact": {
    "holdReason": "Documentation incomplete - missing BOL",
    "resolutionStatus": "PENDING",
    "holdDurationHours": null
  },
  "source_system": "DESCARTES",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **Inventory Allocation:** Remove held shipment from available inventory
- **Replanning:** Source alternative inventory for affected orders
- **Customer Communication:** Auto-generate delay notifications
- **Broker Performance:** Track customs broker resolution times

---

### 4. TRUCK_AVAILABILITY

**Trigger:** `mandala.truck.empty` events (opt-in feature)

**Description:** Truck has completed delivery and is available for new load. Used for dynamic capacity optimization.

**Severity:** Always `INFO`

**Payload Schema:**
```json
{
  "disruption_type": "TRUCK_AVAILABILITY",
  "entity_id": "truck_22222",
  "entity_type": "TRUCK",
  "severity": "INFO",
  "timestamp": "2026-05-09T19:00:00Z",
  "description": "Truck truck_22222 is empty and available for load",
  "impact": {
    "equipmentType": "REEFER_53",
    "gpsLocation": {
      "latitude": 32.7157,
      "longitude": -117.1611
    },
    "carrierDot": "1234567"
  },
  "source_system": "SAMSARA",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **Dynamic Sourcing:** Match available trucks to pending orders
- **Route Optimization:** Reduce empty miles by positioning trucks optimally
- **Load Board Integration:** Push to external load boards (DAT, Truckstop)
- **Carrier Management:** Track carrier capacity utilization

---

### 5. SHIPMENT_STATUS

**Trigger:** `mandala.shipment.status` events with problematic statuses

**Description:** Shipment status indicating delay, hold, or exception.

**Severity Mapping:**
- `WARNING` - DELAYED, HELD, CANCELLED, EXCEPTION
- `INFO` - Other statuses (not sent as disruption)

**Payload Schema:**
```json
{
  "disruption_type": "SHIPMENT_STATUS",
  "entity_id": "shipment_33333",
  "entity_type": "SHIPMENT",
  "severity": "WARNING",
  "timestamp": "2026-05-09T20:00:00Z",
  "description": "Shipment status: DELAYED",
  "impact": {
    "status": "DELAYED",
    "eta": "2026-05-10T12:00:00Z",
    "origin": "LOS_ANGELES_CA",
    "destination": "CHICAGO_IL"
  },
  "source_system": "DESCARTES",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **ETA Replanning:** Update downstream planning based on new ETA
- **Customer Communication:** Proactive delay notifications
- **Inventory Buffering:** Increase safety stock for affected SKUs
- **Carrier Performance:** Track on-time delivery KPIs

---

### 6. CARRIER_RISK

**Trigger:** `mandala.carrier.safety` events (FMCSA enrichment)

**Description:** Carrier safety score update from FMCSA SAFER database. High scores indicate operational risk.

**Severity Mapping:**
- `INFO` - CSA score ≤ 75
- `WARNING` - CSA score 76-90
- `CRITICAL` - CSA score > 90

**Payload Schema:**
```json
{
  "disruption_type": "CARRIER_RISK",
  "entity_id": "1234567",
  "entity_type": "CARRIER",
  "severity": "WARNING",
  "timestamp": "2026-05-09T21:00:00Z",
  "description": "Carrier safety score: 82",
  "impact": {
    "csaScore": 82,
    "inspectionHistory": {
      "inspectionsLast24Months": 12,
      "violationsLast24Months": 3
    },
    "authorityStatus": "ACTIVE"
  },
  "source_system": "FMCSA",
  "correlation_id": "uuid-v7"
}
```

**Maestro Integration Points:**
- **Carrier Selection:** Prefer low-risk carriers in optimization
- **Risk Scoring:** Incorporate CSA scores into carrier risk models
- **Compliance Monitoring:** Flag carriers approaching OOS thresholds
- **Contract Management:** Trigger carrier performance reviews

---

## Integration Scoping Checklist

For Kinaxis integration team to scope the implementation:

### API Requirements
- [ ] **Endpoint:** `/api/disruptions/batch` (or equivalent)
- [ ] **Authentication:** API key or OAuth2 client credentials
- [ ] **Rate Limits:** Define acceptable throughput (e.g., 100 disruptions/minute)
- [ ] **Retry Logic:** Exponential backoff for transient failures
- [ ] **Error Handling:** Dead-letter queue for failed disruptions

### Data Model Alignment
- [ ] Map `entity_type` to Maestro object types (Truck, Shipment, Carrier)
- [ ] Validate `disruption_type` against Maestro disruption taxonomy
- [ ] Align `severity` levels with Maestro alerting framework
- [ ] Define required vs. optional `impact` fields per disruption type

### Business Logic Integration
- [ ] **Replanning Triggers:** Which disruption types should trigger replanning?
- [ ] **KPI Calculations:** How to measure disruption impact on supply chain KPIs?
- [ ] **Alerting Rules:** Define escalation paths for CRITICAL disruptions
- [ ] **Historical Analysis:** Store disruption history for trend analysis

### Performance Considerations
- [ ] **Latency Requirements:** Target < 30 seconds from event to disruption ingestion
- [ ] **Throughput:** Handle peak event rates (e.g., 1,000 events/minute)
- [ ] **Data Retention:** Define disruption retention period (e.g., 90 days)
- [ ] **Backpressure:** Handle Mandala event bursts without dropping data

### Security & Governance
- [ ] **Data Classification:** Classify disruption data sensitivity level
- [ ] **Access Control:** Define who can view/act on disruptions
- [ ] **Audit Trail:** Log all disruption ingestion and processing
- [ ] **Data Residency:** Ensure compliance with data residency requirements

---

## Next Steps

1. **Technical Review:** Kinaxis team reviews SIGNALS.md and connector.py
2. **API Specification:** Kinaxis provides endpoint documentation and auth details
3. **Pilot Testing:** Test with sample Mandala events in staging environment
4. **Production Rollout:** Gradual rollout with monitoring and alerting
5. **Continuous Improvement:** Iterate on disruption types and mapping based on feedback

---

## Contact

For integration questions or to schedule a technical review:
- **GitHub:** https://github.com/theoddden/Mandala
- **Email:** team@mandala.dev (placeholder)

## License

Apache 2.0 - see LICENSE in root directory.
