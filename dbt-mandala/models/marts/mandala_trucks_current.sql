{{ config(materialized='table', tags=['mandala', 'mart'], unique_key='truck_urn') }}

with positions as (
    select * from {{ ref('stg_mandala__truck_positions') }}
),

ranked as (
    select
        *,
        row_number() over (partition by truck_urn order by event_time desc) as rn
    from positions
)

select
    truck_urn,
    truck_id,
    vendor_scope,
    vin,
    license_plate,
    fuel_type,
    latitude,
    longitude,
    heading_deg,
    speed_mps,
    odometer_km,
    fuel_pct,
    soc_pct,
    engine_state,
    captured_at        as last_position_at,
    event_time         as last_event_at
from ranked
where rn = 1
