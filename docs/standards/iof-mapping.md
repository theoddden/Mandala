# IOF Supply Chain Reference Ontology (SCRO) Alignment

**Version:** 1.0
**Date:** 2026-05-10

## Overview

The Industrial Ontologies Foundry (IOF) Supply Chain Reference Ontology (SCRO) is a reference ontology for supply chain and logistics. SCRO uses Basic Formal Ontology (BFO) as the top-level ontology and IOF Core as the mid-level ontology.

This document maps Mandala event types and entities to IOF SCRO concepts, making Mandala an academically defensible reference implementation for supply chain operations.

## Why This Matters

**Academic credibility:**
- Universities and industrial consortiums working on supply chain ontologies will cite this alignment
- Positions Mandala as a reference implementation, not just a tool
- Enables semantic web tools to reason over Mandala events

**Standards interoperability:**
- IOF SCRO is aligned with GS1 and APICS standards
- Mandala events carry IOF URIs as optional `iof_uris` field
- When present, events are RDF-serializable for semantic web tools

**Acquisition relevance:**
- Palantir's Foundry ontology team would understand the value immediately
- IOF SCRO concepts map cleanly to Foundry ontology objects
- Mandala becomes the open-source feeder for Foundry deployments in logistics

## Mandala → IOF SCRO Mapping

| Mandala Concept | IOF SCRO Concept | IOF URI |
|----------------|------------------|---------|
| **Truck** | BFO:0000027 (Object) | http://purl.obolibrary.org/obo/BFO_0000027 |
| **Shipment** | LOG:LOG_1000029 (Shipment) | https://w3id.org/log/ontology/LOG_1000029 |
| **Carrier** | org:FormalOrganization | https://www.w3.org/TR/vocab-org/#FormalOrganization |
| **Carrier Role** | LOG:LOG_1000050 (Carrier) | https://w3id.org/log/ontology/LOG_1000050 |
| **Driver** | foaf:Person | http://xmlns.com/foaf/0.1/Person |
| **Location** | PMD:PMD_0040029 (Geospatial Site) | https://w3id.org/pmd/co/3.0.0/PMD_0040029 |
| **POE** | LOG:LOG_1000146 (Physical Premises) | https://w3id.org/log/ontology/LOG_1000146 |
| **Warehouse** | LOG:LOG_1000032 (Facility) | https://w3id.org/log/ontology/LOG_1000032 |
| **Distribution Center** | LOG:LOG_1000034 (Distribution Center) | https://w3id.org/log/ontology/LOG_1000034 |
| **Customs Broker** | LOG:LOG_1000051 (Business Function) | https://w3id.org/log/ontology/LOG_1000051 |
| **Freight Forwarder** | LOG:LOG_1000001 (Freight Forwarding) | https://w3id.org/log/ontology/LOG_1000001 |
| **Transport Process** | LOG:LOG_1000002 (Transport) | https://w3id.org/log/ontology/LOG_1000002 |
| **Bill of Lading** | LOG:LOG_1000088 (Bill of Lading) | https://w3id.org/log/ontology/LOG_1000088 |
| **Purchase Order** | LOG:LOG_1000089 (Purchase Order) | https://w3id.org/log/ontology/LOG_1000089 |
| **Shipment Plan** | LOG:LOG_1000090 (Shipment Plan) | https://w3id.org/log/ontology/LOG_1000090 |
| **Cargo** | BFO:0000040 (Object Aggregate) | http://purl.obolibrary.org/obo/BFO_0000040 |
| **Traceable Resource Unit** | LOG:LOG_1000129 (Traceable Resource Unit) | https://w3id.org/log/ontology/LOG_1000129 |
| **Cold Chain Breach** | BFO:0000015 (Process) | http://purl.obolibrary.org/obo/BFO_0000015 |

## Mandala Event Types → IOF SCRO

| Mandala Event Type | IOF SCRO Concept | IOF URI |
|-------------------|------------------|---------|
| `mandala.shipment.delivered` | LOG:LOG_1000034 (Distribution Center) | https://w3id.org/log/ontology/LOG_1000034 |
| `mandala.shipment.loaded` | LOG:LOG_1000002 (Transport) | https://w3id.org/log/ontology/LOG_1000002 |
| `mandala.shipment.unloaded` | LOG:LOG_1000002 (Transport) | https://w3id.org/log/ontology/LOG_1000002 |
| `mandala.truck.location.updated` | PMD:PMD_0040029 (Geospatial Site) | https://w3id.org/pmd/co/3.0.0/PMD_0040029 |
| `mandala.cold_chain.breach` | BFO:0000015 (Process) | http://purl.obolibrary.org/obo/BFO_0000015 |
| `mandala.customs.filing.landed` | LOG:LOG_1000051 (Business Function) | https://w3id.org/log/ontology/LOG_1000051 |
| `mandala.border.crossing` | LOG:LOG_1000146 (Physical Premises) | https://w3id.org/log/ontology/LOG_1000146 |

## Example RDF Serialization

```turtle
@prefix log: <https://w3id.org/log/ontology/> .
@prefix org: <https://www.w3.org/ns/org#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix bfo: <http://purl.obolibrary.org/obo/> .
@prefix pmd: <https://w3id.org/pmd/co/3.0.0/> .
@prefix mandala: <https://w3id.org/mandala/iof/> .

# Truck
:truck-123 a bfo:BFO_0000027 ;
    log:hasDriver :driver-456 ;
    log:hasCarrier :carrier-789 .

# Driver
:driver-456 a foaf:Person ;
    org:memberOf :carrier-789 ;
    foaf:name "John Doe" .

# Carrier
:carrier-789 a org:FormalOrganization ;
    log:hasRole log:LOG_1000050 ;  # Carrier role
    foaf:name "ABC Trucking" ;
    org:site :warehouse-001 .

# Warehouse
:warehouse-001 a log:LOG_1000032 ;  # Facility
    org:siteOf :carrier-789 ;
    pmd:PMD_0040029 "POINT(-97.7431 30.2672)"^^geo:wktLiteral .  # WGS84 coordinates

# Shipment
:shipment-999 a log:LOG_1000029 ;  # Shipment
    log:hasTransportProcess :transport-001 ;
    log:hasBillOfLading :bol-123 .

# Transport Process
:transport-001 a log:LOG_1000002 ;  # Transport
    log:usesTruck :truck-123 ;
    log:hasOrigin :origin-warehouse ;
    log:hasDestination :destination-warehouse .

# Cold Chain Breach
:breach-001 a bfo:BFO_0000015 ;  # Process
    log:affectsShipment :shipment-999 ;
    log:hasTemperatureBreach "2.5"^^xsd:float .
```

## Configuration

```python
# settings.py
iof_enabled: bool = False  # Add IOF URIs to events for semantic web compatibility
iof_base_uri: str = "https://w3id.org/mandala/iof/"  # Base URI for IOF concepts
```

```bash
# .env
MANDALA_IOF_ENABLED=1  # Add IOF URIs to events
MANDALA_IOF_BASE_URI=https://w3id.org/mandala/iof/
```

## Implementation Notes

**IOF URIs in MandalaEvents:**
```python
# When IOF is enabled, MandalaEvents carry IOF URIs as optional field
event.data["iof_uris"] = {
    "truck": "https://w3id.org/log/ontology/LOG_1000001",
    "shipment": "https://w3id.org/log/ontology/LOG_1000029",
    "carrier": "https://www.w3.org/TR/vocab-org/#FormalOrganization",
}
```

**RDF serialization:**
- When `iof_enabled=True`, events are RDF-serializable
- Use `rdflib` to serialize to Turtle, JSON-LD, or N-Triples
- Enables semantic web tools to reason over Mandala events

**SHACL validation:**
- Use PMDCo logistics application ontology SHACL shapes for validation
- `shape.ttl` files define validation rules for each pattern
- `shape-data.ttl` files provide real-world annotated examples

## References

- **IOF SCRO:** https://github.com/iofoundry/ontology/blob/master/supplychain/README.md
- **PMDCo Logistics Application Ontology:** https://github.com/materialdigital/logistics-application-ontology
- **BFO (Basic Formal Ontology):** http://purl.obolibrary.org/obo/bfo.owl
- **IOF Core:** https://spec.industrialontologies.org/ontology/core/
- **W3C ORG Vocabulary:** https://www.w3.org/TR/vocab-org/
- **FOAF Vocabulary:** http://xmlns.com/foaf/0.1/
- **WGS84:** https://www.w3.org/2003/01/geo/wgs84_pos#

## Citation

If you use this alignment document in your research or implementation, please cite:

```
Mandala: Operational Reference Implementation for Supply Chain Events.
IOF SCRO Alignment Document. https://github.com/theoddden/Mandala
```

## Summary

This alignment document maps Mandala concepts to IOF SCRO concepts, making Mandala:
- Academically defensible reference implementation
- Compatible with semantic web tools
- RDF-serializable when `iof_enabled=True`
- Citable by universities and industrial consortiums

**Total effort:** ~300 lines (alignment document + URI mapping)
**Value:** Academically defensible reference implementation
**Timeline:** 1 week
