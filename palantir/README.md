# Mandala → Palantir Foundry Integration

## Purpose

This connector translates Mandala's canonical event schema into Palantir Foundry ontology objects, making fleet telemetry and trade/customs events first-class nodes that Foundry's AIP agents can reason over.

## Why This Matters

Mandala bridges Samsara (fleet telemetry) and Descartes (trade/customs) through a single CloudEvents schema. Palantir Foundry's ontology layer is the ideal destination for these events because:

1. **AIP Agent Reasoning**: Every border crossing, cold chain breach, and HOS risk becomes a queryable ontology node
2. **Temporal Context**: Three-timestamp accounting (`time`, `received_at`, `processed_at`) enables precise detection lag measurement
3. **Cross-System Correlation**: Foundry can correlate Mandala events with other supply chain data sources
4. **Enterprise Readiness**: Foundry's data governance and lineage capabilities provide audit trails for regulatory compliance

## Ontology Mapping

### Core Object Types

| Mandala Event Type | Foundry Ontology Object | Key Properties |
|-------------------|------------------------|----------------|
| `mandala.truck.location` | `LogisticsAsset` | `assetId`, `gpsLocation`, `timestamp`, `equipmentType` |
| `mandala.shipment.status` | `Shipment` | `shipmentId`, `status`, `origin`, `destination`, `eta` |
| `mandala.border.crossing` | `BorderCrossing` | `portOfEntry`, `crossingTime`, `customsFiling`, `detectionLag` |
| `mandala.cold_chain.breach` | `ColdChainBreach` | `shipmentId`, `temperature`, `breachWindow`, `regulatoryImpact` |
| `mandala.carrier.safety` | `CarrierProfile` | `dotNumber`, `csaScore`, `inspectionHistory`, `authorityStatus` |
| `mandala.customs.hold` | `CustomsHold` | `shipmentId`, `holdReason`, `holdTime`, `resolutionStatus` |

### Property Mapping

#### LogisticsAsset (from mandala.truck.location)
```python
{
    "rid": "logistics:asset:{truck_id}",
    "properties": {
        "assetId": event.data["truck_id"],
        "assetType": "TRUCK",
        "gpsLocation": {
            "latitude": event.data["latitude"],
            "longitude": event.data["longitude"]
        },
        "equipmentType": event.data["equipment_type"],
        "carrierDot": event.data.get("carrier_dot"),
        "lastSeen": event.time,
        "sourceSystem": "SAMSARA"
    }
}
```

#### Shipment (from mandala.shipment.status)
```python
{
    "rid": "logistics:shipment:{shipment_id}",
    "properties": {
        "shipmentId": event.data["shipment_id"],
        "status": event.data["status"],
        "origin": event.data["origin"],
        "destination": event.data["destination"],
        "eta": event.data.get("eta"),
        "carrierDot": event.data.get("carrier_dot"),
        "lastUpdate": event.time,
        "sourceSystem": "DESCARTES"
    }
}
```

#### BorderCrossing (from mandala.border.crossing)
```python
{
    "rid": "logistics:border_crossing:{event_id}",
    "properties": {
        "portOfEntry": event.data["poe_code"],
        "crossingTime": event.time,
        "truckId": event.data["truck_id"],
        "shipmentId": event.data.get("shipment_id"),
        "customsFiling": event.data.get("customs_filing_id"),
        "detectionLag": event.processed_at - event.received_at,
        "alertLag": event.processed_at - event.time,
        "sourceSystem": "MANDALA"
    }
}
```

#### ColdChainBreach (from mandala.cold_chain.breach)
```python
{
    "rid": "logistics:cold_chain_breach:{event_id}",
    "properties": {
        "shipmentId": event.data["shipment_id"],
        "temperature": event.data["temperature"],
        "declaredRange": event.data["declared_range"],
        "breachWindow": {
            "start": event.data["breach_start"],
            "end": event.data["breach_end"]
        },
        "regulatoryImpact": event.data["regulatory_impact"],
        "breachTime": event.time,
        "sourceSystem": "MANDALA"
    }
}
```

## Integration Architecture

```
┌─────────────┐    MandalaEvent    ┌──────────┐    Foundry Objects    ┌─────────────┐
│  Samsara    │ ──────────────────▶ │ Mandala  │ ─────────────────────▶ │  Palantir   │
│  Descartes  │                     │  Bridge  │                        │  Foundry    │
└─────────────┘                     └──────────┘                        └─────────────┘
                                           │
                                           ▼
                                    Redis Streams
                                           │
                                           ▼
                              palantir/stub_connector.py
                                           │
                                           ▼
                              Foundry Ontology API
```

## Stub Connector

The `stub_connector.py` provides a reference implementation for:

1. **Event Translation**: Maps Mandala CloudEvents to Foundry ontology objects
2. **Batch Ingestion**: Pushes objects to Foundry via REST API or stream endpoint
3. **Error Handling**: Retries and dead-letter queue for failed events
4. **Metrics**: Tracks ingestion latency and success rates

## Configuration

```bash
# Foundry Connection
MANDALA_PALANTIR_ENABLED=1
MANDALA_PALANTIR_API_URL=https://your-foundry.palantir.com
MANDALA_PALANTIR_TOKEN=your-foundry-token
MANDALA_PALANTIR_ONTOLOGY_BRANCH=main

# Ingestion Settings
MANDALA_PALANTIR_BATCH_SIZE=100
MANDALA_PALANTIR_FLUSH_INTERVAL_SEC=30
```

## Next Steps for Full Integration

1. **Foundry Ontology Design**: Work with Palantir to formalize the object types and properties
2. **AIP Agent Configuration**: Define agent queries that leverage Mandala events
3. **Data Governance**: Set up egress policies and marking schemes
4. **Performance Testing**: Validate ingestion throughput and latency requirements
5. **Security**: Configure OAuth2 client credentials for production

## Conversation Starter

This stub connector provides a concrete artifact for VP-level discussions:

> "We've built an open-source event bridge that normalizes fleet telemetry (Samsara) and trade/customs data (Descartes) into a canonical CloudEvents schema. We're proposing a Foundry ontology integration that makes every border crossing, cold chain breach, and carrier safety event a first-class ontology node your AIP agents can reason over. Here's the reference implementation showing the ontology mapping and ingestion pattern."

## License

Apache 2.0 - see LICENSE in root directory.
