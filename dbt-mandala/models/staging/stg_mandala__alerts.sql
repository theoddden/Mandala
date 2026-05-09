{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type like 'mandala.alert.%'
)

select
    event_id,
    event_time,
    ingested_at,
    source,
    event_type                                                       as alert_type,
    subject                                                          as subject_urn,
    {{ mandala_json_get('payload', 'severity') }}                    as severity,
    {{ mandala_json_get('payload', 'reason') }}                      as reason,
    {{ mandala_json_get('payload', 'truck_urn') }}                   as truck_urn,
    {{ mandala_json_get('payload', 'shipment_urn') }}                as shipment_urn,
    {{ mandala_json_get('payload', 'border_poe') }}                  as border_poe,
    {{ mandala_json_get('payload', 'customs_status') }}              as customs_status_at_alert,
    payload                                                          as raw_payload
from events
