# Cross-border alert demo

End-to-end walkthrough of Mandala's killer feature: a Samsara-tracked
truck enters a US-Mexico Port-of-Entry geofence with no matching
customs filing in MacroPoint state — Mandala fires
`mandala.alert.cross_border.no_filing`.

## Run

```bash
docker compose up -d redis           # from the repo root
python examples/cross_border_demo/demo.py
```

You'll see, in order:

1. A simulated MacroPoint `StatusUpdate` posts shipment `S-100` as
   `dispatched` with no customs filing yet.
2. A simulated Samsara `VehicleEnterGeofence` for truck `T-100`
   crossing the Laredo TX POE.
3. Mandala emits `mandala.alert.cross_border.no_filing` with severity
   `high`.
4. We then post a customs `filed` event and re-trigger the geofence.
   No alert this time.

The whole demo runs in-process against a real Redis. No webhooks, no
Samsara/Descartes accounts needed.
