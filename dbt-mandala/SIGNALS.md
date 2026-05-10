# Mandala dbt Marts - Planned and Current

This document describes the current and planned dbt marts in the Mandala warehouse package. Marts are the consumable layer that downstream dashboards, reports, and applications query.

## Current Marts

| Mart | Description | Status |
|------|-------------|--------|
| `mandala_shipments` | One row per shipment with status, customs status, ETA, carrier, broker, latest position, and timeline | ✅ Production |
| `mandala_trucks_current` | Latest known state per truck | ✅ Production |
| `mandala_border_crossings` | Ledger of every Port-of-Entry geofence crossing, joined to customs filing status | ✅ Production |
| `mandala_cold_chain_compliance` | Temperature breaches matched to declared shipment requirements | ✅ Production |
| `mandala_carbon_per_trip` | Measured CO₂ per trip leg from fuel-consumption telemetry | ✅ Production |

## Planned Marts

### `mandala_claim_evidence` (🚧 In Progress)

**Strategic Value:** The single most expensive and most contested claim type in freight insurance is cold chain breach at a border crossing where a customs delay was involved. This mart provides the causation chain evidence that determines which policy pays. In a $51M nuclear verdict environment, this sequence is worth millions to whoever can produce it.

**Use Case:** Insurance claims adjustment, cargo underwriting, freight litigation defense. An adjuster reads it directly. A defense attorney submits it as evidence. An underwriter uses it to set renewal pricing.

**Event Sequence Captured:**
```
mandala.truck.geofence.entered     → occurred_at: 14:32:07
mandala.border.crossing_no_filing  → detected_at: 14:32:09  (2 seconds later)
mandala.customs.hold               → occurred_at: 15:14:33  (42 min later, from Descartes)
mandala.cold_chain.breach          → occurred_at: 16:18:55  (64 min into the hold)
                                   received_at: 16:19:02
                                   processed_at: 16:19:08
```

That four-event sequence with precise timestamps is a causation chain. It proves:
- The customs hold preceded the cold chain breach by 64 minutes
- Mandala detected the breach 7 seconds after it was reported
- No alert was filed before the breach because there was no customs filing to trigger one

**Schema:**
```sql
select
    s.shipment_id,
    s.origin,
    s.destination,
    s.declared_cargo_value,
    s.declared_temperature_range_min,
    s.declared_temperature_range_max,

    -- Causation chain
    bc.occurred_at          as border_crossing_time,
    bc.filing_status        as customs_filing_status,
    bc.detection_lag_sec    as filing_alert_lag_seconds,
    ch.hold_issued_at       as customs_hold_start,
    cc.breach_started_at    as cold_chain_breach_start,
    cc.breach_severity_c    as breach_severity_celsius,

    -- Causation proof
    datediff('minute', bc.occurred_at, cc.breach_started_at)
        as minutes_from_crossing_to_breach,
    datediff('minute', ch.hold_issued_at, cc.breach_started_at)
        as minutes_from_hold_to_breach,

    -- Carrier profile at time of shipment
    csp.csa_score_at_shipment_date,
    csp.out_of_service_rate,
    csp.last_inspection_date,

    -- Alert response
    cc.received_at          as breach_detected_at,
    cc.processed_at         as alert_fired_at,
    datediff('second', cc.breach_started_at, cc.processed_at)
        as total_detection_lag_seconds

from mandala_shipments s
left join mandala_border_crossings bc using (shipment_id)
left join mandala_customs_holds ch using (shipment_id)
left join mandala_cold_chain_compliance cc using (shipment_id)
left join mandala_carrier_safety_profile csp using (dot_number)
where s.shipment_id = '{{ var("claim_shipment_id") }}'
```

**Strategic Position:** TruckerCloud is the bridge for driving behavior. Mandala is the bridge for cross-system causation. These are not the same product and they don't compete. TruckerCloud would actually want to partner with Mandala because their insurer customers are currently missing exactly the customs + cold chain causation layer that Mandala provides.

**Implementation Status:** 🚧 Planned - Schema defined, pending implementation

---

## Future Planned Marts

### `mandala_carrier_safety_profile` (📋 Backlog)

**Description:** Live CSA scores, inspection history, FMCSA authority status per DOT number. Enables carrier risk scoring and safety compliance monitoring.

**Use Case:** Carrier selection optimization, risk underwriting, compliance monitoring.

**Status:** 📋 Backlog

---

### `mandala_intermodal_legs` (📋 Backlog)

**Description:** Rail status, ETA, last free day, milestones per container. Enables intermodal visibility and demurrage/detention management.

**Use Case:** Intermodal shipment tracking, rail carrier performance, demurrage cost optimization.

**Status:** 📋 Backlog

---

## Contributing New Marts

To propose a new mart:

1. Add it to this SIGNALS.md file with:
   - Description and strategic value
   - Use case and target audience
   - Proposed schema (SQL or table structure)
   - Implementation status (🚧 Planned, 📋 Backlog, ✅ Production)

2. Open a GitHub issue with the title: `New mart proposal: <mart_name>`

3. Tag the issue with `enhancement` and `mart`

4. The maintainers will review and provide feedback on:
   - Alignment with canonical schema
   - Data availability from existing staging/intermediate models
   - Test coverage requirements
   - Documentation completeness

---

## Contact

For questions about marts or to propose new ones:
- **GitHub:** https://github.com/theoddden/Mandala
- **Issues:** https://github.com/theoddden/Mandala/issues

## License

Apache 2.0 - see LICENSE in root directory.
