{{ config(materialized='table', tags=['mandala', 'mart']) }}

-- Ledger of every border-POE geofence crossing, joined to customs state
-- at the moment of crossing. Useful for retroactive audits ("which
-- crossings happened with no customs filing?").

with crossings as (
    select * from {{ ref('stg_mandala__geofence_crossings') }}
    where lower(coalesce(geofence_name, '')) like '%border%'
       or lower(coalesce(geofence_name, '')) like '%poe%'
       or geofence_id in (select poe_code from {{ ref('mandala_border_pois') }})
),

shipments as (
    select * from {{ ref('mandala_shipments') }}
),

-- Truck → shipment association via handoff events. v0.1 simplification:
-- pick the most recently dispatched shipment for the same vendor scope.
-- Replace with a proper handoff projection in v0.2.
ranked_ship as (
    select
        s.*,
        row_number() over (partition by s.vendor_scope order by s.last_event_at desc) as rn
    from shipments s
)

select
    c.event_id,
    c.event_time         as crossed_at,
    c.direction,
    c.truck_urn,
    c.geofence_id        as poe_code,
    c.geofence_name      as poe_name,
    rs.shipment_urn,
    rs.status            as shipment_status_at_crossing,
    rs.customs_status    as customs_status_at_crossing,
    rs.customs_authority,
    rs.customs_entry_number
from crossings c
left join ranked_ship rs
       on rs.rn = 1
