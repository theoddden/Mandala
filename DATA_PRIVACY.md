# Data Privacy

> **Mandala is a connector library, not a data store.**

Mandala is designed to *pass through* fleet and trade events between
systems you already operate. It does not maintain a persistent database
of shipment or driver data. The only state Mandala keeps is short-TTL
projection in Redis (default: **14 days**) so the cross-border alert
engine and MCP tools can answer questions like "is there a customs filing
for the truck currently entering this geofence?".

This page describes Mandala's data-handling posture and your
responsibilities as the operator.

## What Mandala stores

| Data | Where | TTL | Purpose |
|---|---|---|---|
| Last-known truck position | Redis | 14 days | Alert engine + MCP `get_truck` |
| Shipment status / customs status | Redis | 14 days | Alert engine + MCP `get_shipment` |
| Shipment timeline (last 1000 events) | Redis | 14 days | MCP `get_shipment` |
| Truck ↔ shipment links | Redis | 14 days | Cross-border alert correlation |
| Idempotency keys (hash of webhook ID) | Redis | 24 hours | Webhook dedupe |
| The event stream itself | Redis Streams | capped at 100k events | Worker consumption |

Mandala does **not** store driver names, license numbers, phone numbers,
or vehicle VINs in long-lived state by default. Those fields pass
through events to subscribers and the warehouse sink, but Mandala itself
does not project them into the state store.

## Your responsibilities

### Cross-border data transfers (GDPR, UK GDPR, Swiss FADP)

Vehicle telematics — including position, speed, harsh-event detection,
and HOS records — is **personal data** under GDPR when it can identify a
driver, even indirectly. If you operate trucks in the EU/UK/CH and
forward Mandala events to a non-adequate-decision country (e.g. the US),
you are responsible for:

- a lawful basis for processing,
- transfer mechanisms (SCCs, BCRs, or adequacy),
- a Data Processing Agreement with any downstream processors,
- and informing drivers per Art. 13/14 GDPR.

Mandala does not perform any of these for you.

### CCPA / CPRA

If you operate California fleets, driver data is "personal information"
under CCPA. Carriers must honour driver requests for access, deletion,
and limits on use of sensitive personal information. Mandala's TTL-based
state means a deletion request from a driver propagates within 14 days
naturally; for faster compliance, run `mandala admin redact <urn>` (TODO,
see `examples/redact.py`).

### Driver consent

Some jurisdictions (Germany BetrVG, France CNIL, Quebec) require works-
council or employee-representative consent for telematics-based
behaviour scoring. Mandala forwards harsh-event and HOS-violation
events; you must ensure your collection upstream is lawful before those
events reach Mandala.

## Anonymization mode

Mandala supports an opt-in `data_anonymization` mode that strips PII
before publishing events to the bus. When enabled (`MANDALA_ANONYMIZE=1`,
planned for v0.2):

- driver `name`, `phone`, `license_number` → omitted
- truck `vin`, `license_plate` → omitted
- `subject` URNs are HMAC'd with a per-deployment salt before emission

This is a defense-in-depth feature, not a substitute for proper data-
processing agreements.

## What Mandala does *not* do

- It does not transmit data to Mandala maintainers, telemetry endpoints,
  or third parties. There is no phone-home.
- It does not log full event payloads at INFO level. Only event type,
  subject URN, and the result of processing are logged.
- It does not retain raw inbound webhook bodies after normalization.

## Reporting concerns

Security or privacy issues: please email **security@mandala-bridge.dev**
or use GitHub's private vulnerability reporting on the repository. Do
not file public issues for security concerns.
