"""Logistics semantic conventions for OpenTelemetry attributes.

When :class:`mandala.core.events.envelope.MandalaEvent` is serialized as an
OTel span, its ``attributes`` dict carries domain-specific keys. We namespace
under ``logistics.*`` (proposed contribution to OTel semantic conventions)
and ``mandala.*`` (vendor-specific).

Use the constants from this module rather than raw strings so vendor changes
propagate consistently:

    from mandala.core.events.semconv import LogisticsAttr

    event = new_event(
        type="mandala.truck.geofence.entered",
        source="mandala/connector/samsara",
        subject="urn:mandala:shipment:ABC123",
        attributes={
            LogisticsAttr.SHIPMENT_ID: "ABC123",
            LogisticsAttr.TRUCK_ID: "truck-42",
            LogisticsAttr.CARRIER_SCAC: "MAEU",
            LogisticsAttr.LOCATION_POE: "laredo",
        },
    )

These attributes appear in any OTLP-compatible backend (Jaeger, Tempo,
Honeycomb, Datadog, Grafana Cloud) as first-class facets — meaning every
shipment is filterable / groupable / aggregable using the observability
tooling your platform team already runs.
"""

from __future__ import annotations

from typing import Final


class LogisticsAttr:
    """Semantic convention keys for logistics spans.

    Stable across Mandala minor versions. Additions only; removals require a
    major version bump.
    """

    # --- Shipment identity -------------------------------------------------
    SHIPMENT_ID: Final = "logistics.shipment.id"
    SHIPMENT_REFERENCE: Final = "logistics.shipment.reference"  # BOL, PRO, etc.
    SHIPMENT_MODE: Final = "logistics.shipment.mode"  # truck|rail|vessel|air

    # --- Carrier -----------------------------------------------------------
    CARRIER_NAME: Final = "logistics.carrier.name"
    CARRIER_SCAC: Final = "logistics.carrier.scac"
    CARRIER_DOT: Final = "logistics.carrier.dot_number"

    # --- Truck / driver ---------------------------------------------------
    TRUCK_ID: Final = "logistics.truck.id"
    TRUCK_VIN: Final = "logistics.truck.vin"
    DRIVER_ID: Final = "logistics.driver.id"
    DRIVER_NAME: Final = "logistics.driver.name"

    # --- Container / equipment --------------------------------------------
    CONTAINER_ID: Final = "logistics.container.id"
    EQUIPMENT_TYPE: Final = "logistics.equipment.type"

    # --- Vessel -----------------------------------------------------------
    VESSEL_IMO: Final = "logistics.vessel.imo"
    VESSEL_MMSI: Final = "logistics.vessel.mmsi"
    VESSEL_NAME: Final = "logistics.vessel.name"

    # --- Rail -------------------------------------------------------------
    RAIL_CARRIER: Final = "logistics.rail.carrier"  # UP, BNSF, CSX, NS, CN, CPKC
    RAIL_RAMP: Final = "logistics.rail.ramp"

    # --- Location ---------------------------------------------------------
    LOCATION_LAT: Final = "logistics.location.lat"
    LOCATION_LON: Final = "logistics.location.lon"
    LOCATION_POE: Final = "logistics.location.poe"  # port-of-entry code
    LOCATION_PORT: Final = "logistics.location.port"  # UN/LOCODE
    LOCATION_FACILITY: Final = "logistics.location.facility"
    LOCATION_GEOFENCE: Final = "logistics.location.geofence"

    # --- Customs ----------------------------------------------------------
    CUSTOMS_STATUS: Final = "logistics.customs.status"  # filed|hold|cleared|...
    CUSTOMS_HOLD_REASON: Final = "logistics.customs.hold_reason"
    CUSTOMS_FILING_ID: Final = "logistics.customs.filing_id"

    # --- Cold chain -------------------------------------------------------
    COLD_CHAIN_TEMP_C: Final = "logistics.cold_chain.temperature_c"
    COLD_CHAIN_THRESHOLD_LO_C: Final = "logistics.cold_chain.threshold_low_c"
    COLD_CHAIN_THRESHOLD_HI_C: Final = "logistics.cold_chain.threshold_high_c"

    # --- Fuel / cost ------------------------------------------------------
    FUEL_GALLONS: Final = "logistics.fuel.gallons"
    FUEL_COST_USD: Final = "logistics.fuel.cost_usd"
    FUEL_VENDOR: Final = "logistics.fuel.vendor"  # coast|fleetcor|wex|efs
