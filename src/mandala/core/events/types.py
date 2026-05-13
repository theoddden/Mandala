"""Mandala CloudEvents ``type`` registry.

Every type is a dot-separated string in the ``mandala.<entity>.<verb>`` form.
Adding a new type is a one-line addition here plus the matching ``data``
schema in :mod:`mandala.core.schema`.
"""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    # --- Truck telemetry (Samsara, Geotab, Motive, ...) -------------------
    TRUCK_POSITION = "mandala.truck.position.updated"
    TRUCK_GEOFENCE_ENTERED = "mandala.truck.geofence.entered"
    TRUCK_GEOFENCE_EXITED = "mandala.truck.geofence.exited"
    TRUCK_POE_ENTERED = "mandala.truck.poe.entered"  # Port-of-Entry geofence entered
    TRUCK_POE_EXITED = "mandala.truck.poe.exited"  # Port-of-Entry geofence exited
    TRUCK_ETA_UPDATED = "mandala.truck.eta.updated"
    TRUCK_HARSH_EVENT = "mandala.truck.harsh_event.detected"
    TRUCK_FUEL_LOW = "mandala.truck.fuel.low"
    TRUCK_FUELED = "mandala.truck.fueled"
    TRUCK_DOOR_OPENED = "mandala.truck.door.opened"

    # --- Cold chain --------------------------------------------------------
    COLD_CHAIN_READING = "mandala.truck.cold_chain.reading"
    COLD_CHAIN_BREACH = "mandala.truck.cold_chain.breach"
    COLD_CHAIN_RECOVERED = "mandala.truck.cold_chain.recovered"

    # --- Driver / HOS -----------------------------------------------------
    DRIVER_ASSIGNED = "mandala.driver.assigned"
    DRIVER_HOS_WARNING = "mandala.driver.hos.warning"
    DRIVER_LOG_VIOLATION = "mandala.driver.hos.violation"

    # --- Shipment lifecycle (Descartes GLN, MacroPoint) ------------------
    SHIPMENT_BOOKED = "mandala.shipment.booked"
    SHIPMENT_DISPATCHED = "mandala.shipment.dispatched"
    SHIPMENT_PICKED_UP = "mandala.shipment.picked_up"
    SHIPMENT_IN_TRANSIT = "mandala.shipment.in_transit"
    SHIPMENT_AT_BORDER = "mandala.shipment.at_border"
    SHIPMENT_DELIVERED = "mandala.shipment.delivered"
    SHIPMENT_CANCELLED = "mandala.shipment.cancelled"
    SHIPMENT_ETA_UPDATED = "mandala.shipment.eta.updated"
    SHIPMENT_HANDOFF = "mandala.shipment.handoff.confirmed"
    SHIPMENT_DOCK_READY = "mandala.shipment.dock.ready"
    SHIPMENT_DOCK_BLOCKED = "mandala.shipment.dock.blocked"

    # --- Customs (Descartes GLN, CBP, SAT, CBSA) -------------------------
    CUSTOMS_FILED = "mandala.shipment.customs.filed"
    CUSTOMS_HOLD = "mandala.shipment.customs.hold"
    CUSTOMS_HOLD_LANDED = "mandala.shipment.customs.hold.landed"
    CUSTOMS_HOLD_CLEARED = "mandala.shipment.customs.hold.cleared"
    CUSTOMS_EXAM = "mandala.shipment.customs.exam"
    CUSTOMS_RELEASED = "mandala.shipment.customs.released"
    CUSTOMS_REJECTED = "mandala.shipment.customs.rejected"
    CUSTOMS_DOCUMENTATION_MISSING = "mandala.shipment.customs.documentation.missing"
    CUSTOMS_INSPECTION_REQUIRED = "mandala.shipment.customs.inspection.required"

    # --- BOL / paperwork --------------------------------------------------
    BOL_RECEIVED = "mandala.shipment.bol.received"
    BOL_AMENDED = "mandala.shipment.bol.amended"

    # --- Compliance (Descartes Visual Compliance / Denied Party) ---------
    PARTY_SCREENED_CLEAR = "mandala.party.screened.clear"
    PARTY_SCREENED_FLAGGED = "mandala.party.screened.flagged"

    # --- Trade intelligence (Descartes Datamyne) -------------------------
    TRADE_LANE_INSIGHT = "mandala.trade.lane.insight"

    # --- Capacity / load board --------------------------------------------
    TRUCK_EMPTY = "mandala.truck.empty"
    TRUCK_EMPTY_VERIFIED = "mandala.truck.empty.verified"
    TRUCK_AVAILABLE = "mandala.truck.available"
    LOADBOARD_POSTED = "mandala.loadboard.posted"
    LOADBOARD_POST_FAILED = "mandala.loadboard.post_failed"
    LOADBOARD_EXPIRED = "mandala.loadboard.expired"

    # --- Carrier enrichment (FMCSA SAFER) ----------------------------------
    CARRIER_FMCSA_ENRICHED = "mandala.carrier.fmcsa.enriched"

    # --- Rail / intermodal (Vizion API) ------------------------------------
    RAIL_INGATE = "mandala.rail.ingate"
    RAIL_OUTGATE = "mandala.rail.outgate"
    RAIL_ARRIVAL = "mandala.rail.arrival"
    RAIL_DEPARTURE = "mandala.rail.departure"
    RAIL_AVAILABLE_FOR_PICKUP = "mandala.rail.available_for_pickup"
    RAIL_LAST_FREE_DAY = "mandala.rail.last_free_day"
    RAIL_INTERMODAL_STATUS = "mandala.rail.intermodal_status"

    # --- Internal / system ------------------------------------------------
    PLAYBOOK_TRIGGERED = "mandala.playbook.triggered"
    PLAYBOOK_ACTION_OK = "mandala.playbook.action.ok"
    PLAYBOOK_ACTION_FAILED = "mandala.playbook.action.failed"

    # --- Trailer entity (Laredo drayage swap) ---------------------------
    TRAILER_CREATED = "mandala.trailer.created"
    TRAILER_LOCATION_UPDATED = "mandala.trailer.location.updated"
    PLAYBOOK_ACTION_OK = "mandala.playbook.action.ok"
    PLAYBOOK_ACTION_FAILED = "mandala.playbook.action.failed"
