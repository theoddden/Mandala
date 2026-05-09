{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type in (
        'mandala.truck.geofence.entered',
        'mandala.truck.geofence.exited'
    )
)

select
    event_id,
    event_time,
    ingested_at,
    source,
    event_type,
    case
        when event_type = 'mandala.truck.geofence.entered' then 'entered'
        else 'exited'
    end                                                              as direction,
    subject                                                          as truck_urn,
    subject_id                                                       as truck_id,
    {{ mandala_json_get('payload', 'geofence_id') }}                 as geofence_id,
    {{ mandala_json_get('payload', 'geofence_name') }}               as geofence_name,
    {{ mandala_json_get_timestamp('payload', 'occurred_at') }}       as occurred_at,
    {{ mandala_json_get('payload', 'vendor') }}                      as vendor
from events
