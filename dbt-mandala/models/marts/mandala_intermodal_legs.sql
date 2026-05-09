{{
  config(
    materialized = 'table',
    tags = ['mandala', 'mart'],
    unique_key = 'container_id'
  )
}}

-- mandala_intermodal_legs
-- Joins rail events onto the shipment timeline.
-- Grain: one row per intermodal leg (container + origin_ramp + destination_ramp).
-- Requires MANDALA_VIZION_API_KEY to be set for events to populate.

with rail_events as (
    select
        json_extract_scalar(data, '$.container_id')       as container_id,
        json_extract_scalar(data, '$.carrier_scac')        as rail_carrier_scac,
        json_extract_scalar(data, '$.origin_ramp')         as origin_ramp,
        json_extract_scalar(data, '$.destination_ramp')    as destination_ramp,
        cast(json_extract_scalar(data, '$.eta')
            as timestamp)                                   as eta,
        cast(json_extract_scalar(data, '$.last_free_day')
            as timestamp)                                   as last_free_day,
        cast(json_extract_scalar(data,
            '$.available_for_pickup') as boolean)          as available_for_pickup,
        json_extract_scalar(data, '$.provider')            as provider,
        time                                                as event_time
    from {{ ref('stg_mandala_events') }}
    where type like 'mandala.rail.%'
),

shipments as (
    select
        shipment_id,
        container_id,
        carrier_scac  as truck_carrier_scac
    from {{ ref('mandala_shipments') }}
    where container_id is not null
)

select
    s.shipment_id,
    r.container_id,
    r.rail_carrier_scac,
    s.truck_carrier_scac,
    r.origin_ramp,
    r.destination_ramp,
    r.eta,
    r.last_free_day,
    r.available_for_pickup,
    r.provider,
    r.event_time,
    -- days until last free day — negative = already in demurrage
    datediff('day', current_timestamp, r.last_free_day) as days_until_lfd
from rail_events r
left join shipments s using (container_id)
