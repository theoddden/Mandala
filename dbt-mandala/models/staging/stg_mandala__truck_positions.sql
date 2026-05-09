{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type = 'mandala.truck.position.updated'
)

select
    event_id,
    event_time,
    ingested_at,
    source,
    subject                                                            as truck_urn,
    subject_id                                                         as truck_id,
    subject_scope                                                      as vendor_scope,
    {{ mandala_json_get_number('payload', 'position.point.lat') }}     as latitude,
    {{ mandala_json_get_number('payload', 'position.point.lon') }}     as longitude,
    {{ mandala_json_get_number('payload', 'position.point.heading_deg') }} as heading_deg,
    {{ mandala_json_get_number('payload', 'position.point.speed_mps') }}   as speed_mps,
    {{ mandala_json_get_number('payload', 'position.odometer_km') }}   as odometer_km,
    {{ mandala_json_get_number('payload', 'position.fuel_pct') }}      as fuel_pct,
    {{ mandala_json_get_number('payload', 'position.soc_pct') }}       as soc_pct,
    {{ mandala_json_get('payload', 'position.engine_state') }}         as engine_state,
    {{ mandala_json_get('payload', 'truck.vin') }}                     as vin,
    {{ mandala_json_get('payload', 'truck.license_plate') }}           as license_plate,
    {{ mandala_json_get('payload', 'truck.fuel_type') }}               as fuel_type,
    {{ mandala_json_get_timestamp('payload', 'position.captured_at') }} as captured_at
from events
