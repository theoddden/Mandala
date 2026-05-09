{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type in (
        'mandala.truck.cold_chain.reading',
        'mandala.truck.cold_chain.breach',
        'mandala.truck.cold_chain.recovered'
    )
)

select
    event_id,
    event_time,
    ingested_at,
    subject                                                            as truck_urn,
    subject_id                                                         as truck_id,
    case when event_type = 'mandala.truck.cold_chain.breach' then true else false end as is_breach,
    {{ mandala_json_get('payload', 'sensor_id') }}                    as sensor_id,
    {{ mandala_json_get_number('payload', 'temperature_c') }}         as temperature_c,
    {{ mandala_json_get_number('payload', 'humidity_pct') }}          as humidity_pct,
    {{ mandala_json_get_number('payload', 'setpoint_c') }}            as setpoint_c,
    {{ mandala_json_get('payload', 'door_open') }}                    as door_open,
    {{ mandala_json_get_timestamp('payload', 'captured_at') }}        as captured_at
from events
