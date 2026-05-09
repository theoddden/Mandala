"""End-to-end cross-border alert demo.

Run with a local Redis on localhost:6379 (``docker compose up -d redis``):

    python examples/cross_border_demo/demo.py

The demo:

1. Links truck ``T-100`` to shipment ``S-100`` via the StateStore
   (simulating a handoff event).
2. Marks the shipment as ``dispatched`` with no customs filing yet.
3. Fires a synthetic Samsara ``VehicleEnterGeofence`` for the Laredo
   POE — Mandala emits ``mandala.alert.cross_border.no_filing``.
4. Marks the shipment as ``customs.filed`` and re-fires the geofence.
   No alert this time (and a slightly different fence to bypass debounce).
"""
from __future__ import annotations

import asyncio
import json
import sys

import redis.asyncio as redis

from mandala.alerts import cross_border
from mandala.connectors.samsara.normalize import normalize as samsara_normalize
from mandala.connectors.descartes.macropoint.normalize import normalize as macropoint_normalize
from mandala.core.state import StateStore
from mandala.projection import project


TRUCK_URN = "urn:mandala:truck:samsara:T-100"
SHIPMENT_URN = "urn:mandala:shipment:macropoint:S-100"


def _emit(label: str, obj) -> None:
    print(f"\n=== {label} ===")
    if hasattr(obj, "to_dict"):
        print(json.dumps(obj.to_dict(), indent=2, default=str))
    else:
        print(json.dumps(obj, indent=2, default=str))


async def main() -> None:
    r = redis.from_url("redis://localhost:6379/0", decode_responses=False)
    state = StateStore(r, ttl_seconds=300)

    # 1. Link truck to shipment (simulates a confirmed handoff).
    await state.link(TRUCK_URN, SHIPMENT_URN)

    # 2. Project a MacroPoint StatusUpdate -> shipment is dispatched, no customs.
    mp_payload = {
        "MessageId": "demo-1",
        "MessageType": "StatusUpdate",
        "Body": {"ShipmentId": "S-100", "Status": "Dispatched", "CarrierScac": "ABCD"},
    }
    for ev in macropoint_normalize(mp_payload):
        await project(ev, state)
    _emit("Shipment after dispatch", await state.get("shipment", SHIPMENT_URN))

    # 3. Samsara: truck enters Laredo POE.
    sam_payload = {
        "eventId": "demo-geofence-1",
        "eventType": "VehicleEnterGeofence",
        "happenedAtTime": "2026-05-08T22:00:00Z",
        "data": {
            "vehicle": {"id": "T-100"},
            "address": {"id": "laredo-1", "name": "Laredo TX Border POE"},
        },
    }
    [geofence_event] = samsara_normalize(sam_payload)
    await project(geofence_event, state)
    alerts = await cross_border(geofence_event, state, r)
    if not alerts:
        print("\nUnexpected: no alert produced. Bailing.")
        sys.exit(1)
    _emit("ALERT (no customs filing)", alerts[0])

    # 4. File customs, re-fire (different fence id avoids debounce).
    customs_payload = {
        "MessageId": "demo-2",
        "MessageType": "StatusUpdate",
        "Body": {"ShipmentId": "S-100", "Status": "InTransit"},
    }
    for ev in macropoint_normalize(customs_payload):
        await project(ev, state)
    await state.upsert("shipment", SHIPMENT_URN, {"customs_status": "filed"})
    _emit("Shipment after customs filed", await state.get("shipment", SHIPMENT_URN))

    sam_payload2 = dict(sam_payload)
    sam_payload2["eventId"] = "demo-geofence-2"
    sam_payload2["data"] = {
        "vehicle": {"id": "T-100"},
        "address": {"id": "el-paso-1", "name": "El Paso TX Border POE"},
    }
    [geofence_event2] = samsara_normalize(sam_payload2)
    alerts2 = await cross_border(geofence_event2, state, r)
    print(f"\n=== Second crossing (filed customs) — alerts emitted: {len(alerts2)} ===")
    if not alerts2:
        print("OK — no alert when customs filing exists.")

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
