# Aurora Integration (Autonomous Trucks)

**Status: Partnership Required**

Aurora Innovation is the leading autonomous trucking company, operating the Aurora Driver in Texas and expanding across North America. Aurora Beacon is their cloud-based mission control system for autonomous fleet operations.

## Why Aurora Matters for 2028

**Autonomous truck scaling:**
- Aurora is already hauling customer loads in Texas
- Mixed fleets (autonomous + human-driven) become standard by 2028
- Human-in-the-loop shifts from driving to monitoring/exception handling

**Asymmetric advantage:**
- Aurora road/traffic/weather intelligence benefits human-driven Samsara trucks
- Mandala provides unified visibility across autonomous + human fleets
- First-mover advantage in autonomous transition (2027-2030)

## Integration Pattern

**Aurora does not have a public API.** Integration requires:
1. Partnership with Aurora (via Aurora Partner Program)
2. API credentials (webhook secret + API key)
3. Business agreement

**When partnership available, implement your own ingestion:**

### Ingestion Pattern

```python
# POST to Mandala /events endpoint
import requests

event = {
    "id": str(uuid.uuid4()),
    "source": "mandala/connector/aurora",
    "type": "mandala.truck.location.updated",
    "specversion": "1.0",
    "time": datetime.now(UTC).isoformat(),
    "subject": f"urn:mandala:truck:aurora:{truck_id}",
    "datacontenttype": "application/json",
    "data": {
        "truck_id": "aur-12345",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "mode": "autonomous",  # autonomous, supervised, manual
        "fuel_level": 0.75,
        "cargo_temp": 2.5,
        "health_status": "normal",  # normal, warning, critical
        "eta_minutes": 180,
        "planned_route": ["point_a", "point_b", "point_c"],
        "current_speed_mph": 65,
    }
}

response = requests.post(
    "https://your-mandala-instance.com/events",
    json=event,
    headers={"Authorization": f"Bearer {MANDALA_API_KEY}"}
)
```

### Aurora Event Types

**Location updates:**
- `mandala.truck.location.updated` - GPS position, mode, health, ETA

**Mode changes:**
- `mandala.truck.mode.changed` - autonomous → manual intervention

**Health status:**
- `mandala.truck.health.warning` - degradation detected
- `mandala.truck.health.critical` - immediate attention required

### Mandala Detectors (Autonomous-Specific)

**1. `cross_border_autonomous` detector**
```python
# Fires when autonomous truck enters POE without customs filing
if event.source == "aurora" and event.type == "mandala.truck.geofence.entered":
    if not state.get(f"customs_filing:{event.data.truck_id}"):
        alert = {
            "type": "mandala.alert.cross_border.autonomous",
            "message": f"Autonomous truck {event.data.truck_id} entering POE without customs filing",
            "severity": "critical",
            "action_required": "file customs immediately",
        }
        # Route to customs broker + fleet manager
```

**2. `autonomous_handover_required` detector**
```python
# Fires when autonomous mode → manual intervention needed
if event.source == "aurora" and event.data.mode == "manual":
    if event.data.previous_mode == "autonomous":
        alert = {
            "type": "mandala.alert.autonomous.handover",
            "message": f"Autonomous truck {event.data.truck_id} requires manual intervention",
            "severity": "high",
            "location": event.data.current_location,
        }
        # Route to fleet manager
```

**3. `charging_infrastructure_constraint` detector**
```python
# Fires when electric autonomous truck cannot reach next charger
if event.source == "aurora" and event.data.fuel_level < 0.20:
    next_charger = find_nearest_charger(event.data.location)
    if distance_to_charger > range_remaining:
        alert = {
            "type": "mandala.alert.charging.constraint",
            "message": f"Autonomous truck {event.data.truck_id} cannot reach next charger",
            "severity": "critical",
            "recommended_action": "reroute to alternate charger",
        }
        # Route to charging network + fleet manager
```

**4. `climate_route_disruption` detector**
```python
# Fires when extreme weather on planned route
if event.source == "aurora":
    weather_data = await climate_api.get_forecast(event.data.planned_route)
    if weather_data.has_extreme_weather:
        alert = {
            "type": "mandala.alert.climate.route_disruption",
            "message": f"Extreme weather on route for {event.data.truck_id}",
            "severity": "high",
            "weather_type": weather_data.type,
            "recommended_action": "reroute or delay",
        }
        # Route to fleet manager
```

### Intelligence Sharing (Aurora → Samsara)

**Aurora's road/traffic/weather data enriches Samsara trucks:**
```python
# Aurora road intelligence stored in Redis
state.set(f"road_intelligence:{route_id}", {
    "traffic_congestion": event.data.traffic_level,
    "weather_conditions": event.data.weather,
    "last_updated": event.time,
})

# Samsara trucks query road intelligence
if event.source == "samsara":
    route_id = event.data.planned_route
    road_intel = state.get(f"road_intelligence:{route_id}")
    if road_intel:
        # Enrich Samsara event with Aurora intelligence
        event.data["aurora_road_intelligence"] = road_intel
```

## Configuration (When Partnership Available)

```python
# settings.py
aurora_enabled: bool = False  # Disabled by default
aurora_webhook_secret: str = ""
aurora_api_key: str = ""
aurora_beacon_enabled: bool = False  # Aurora Beacon platform
aurora_intelligence_sharing: bool = True  # Share Aurora data with Samsara trucks
```

```bash
# .env
MANDALA_AURORA_ENABLED=1
MANDALA_AURORA_WEBHOOK_SECRET=your-secret
MANDALA_AURORA_API_KEY=your-api-key
```

## Why This Wins for 2028

**Unified visibility:**
- Single pane of glass for autonomous + human fleets
- Samsara users get autonomous truck visibility
- Aurora users get customs visibility (via Descartes)

**Intelligence sharing:**
- Aurora road/traffic/weather data benefits Samsara drivers
- Samsara fleet telemetry benefits Aurora dispatch
- Cross-intelligence that neither platform provides alone

**First-mover advantage:**
- Mandala ready when Aurora scales (2027-2030)
- Autonomous truck integration documented and tested
- No competitor has unified autonomous + human fleet visibility

## Partnership Path

**To integrate with Aurora:**
1. Apply to Aurora Partner Program: https://aurora.tech/partners
2. Request logistics integration access
3. Obtain webhook secret + API key
4. Implement ingestion pattern (POST to `/events`)
5. Implement autonomous-specific detectors
6. Enable intelligence sharing (Aurora → Samsara)

**Timeline:**
- 2025-2026: Document integration pattern (this stub)
- 2027: Aurora partnership + API access
- 2028: Full autonomous truck integration
- 2029-2030: Scale with Aurora fleet growth

## Alternative: Waymo Via, Kodiak

**Other autonomous truck players:**
- Waymo Via (Alphabet) - partnership required
- Kodiak Robotics - partnership required
- Plus.ai (autonomous trucks) - partnership required

**Pattern is the same:**
- Document integration pattern (stub)
- Implement when partnership available
- POST to `/events` endpoint
- Autonomous-specific detectors

**Aurora is first priority:**
- Already hauling customer loads in Texas
- Aurora Beacon platform available
- Strong partner program
- First to scale in autonomous truck market

## Summary

Aurora integration is documented as a pattern (stub) because:
- Aurora requires partnership (not public API)
- Aurora is the leading autonomous truck player
- Integration pattern is clear (webhook → POST /events)
- Autonomous-specific detectors are documented
- First-mover advantage when Aurora scales

**When Aurora partnership available:**
1. Enable Aurora config in settings.py
2. Implement webhook ingestion (POST to `/events`)
3. Implement autonomous-specific detectors
4. Enable intelligence sharing (Aurora → Samsara)
5. Test with Aurora Beacon platform

**This is the one hook for autonomous driving.**
