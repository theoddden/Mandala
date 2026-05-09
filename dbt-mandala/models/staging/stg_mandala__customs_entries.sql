{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type in (
        'mandala.shipment.customs.filed',
        'mandala.shipment.customs.hold',
        'mandala.shipment.customs.exam',
        'mandala.shipment.customs.released',
        'mandala.shipment.customs.rejected'
    )
)

select
    event_id,
    event_time,
    ingested_at,
    source,
    event_type,
    case event_type
        when 'mandala.shipment.customs.filed'    then 'filed'
        when 'mandala.shipment.customs.hold'     then 'hold'
        when 'mandala.shipment.customs.exam'     then 'exam'
        when 'mandala.shipment.customs.released' then 'released'
        when 'mandala.shipment.customs.rejected' then 'rejected'
    end                                                              as customs_status,
    subject                                                          as shipment_urn,
    subject_id                                                       as shipment_id,
    {{ mandala_json_get('payload', 'authority') }}                   as authority,
    {{ mandala_json_get('payload', 'entry_number') }}                as entry_number,
    {{ mandala_json_get('payload', 'hold_reason') }}                 as hold_reason,
    {{ mandala_json_get_number('payload', 'duty_owed_usd') }}        as duty_owed_usd,
    {{ mandala_json_get('payload', 'broker.name') }}                 as broker_name,
    {{ mandala_json_get('payload', 'importer.name') }}               as importer_name,
    {{ mandala_json_get_timestamp('payload', 'filed_at') }}          as filed_at,
    {{ mandala_json_get_timestamp('payload', 'released_at') }}       as released_at
from events
