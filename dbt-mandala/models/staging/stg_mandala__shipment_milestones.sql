{{ config(materialized='view', tags=['mandala', 'staging']) }}

with events as (
    select * from {{ ref('stg_mandala__events') }}
    where event_type like 'mandala.shipment.%'
      and event_type not like 'mandala.shipment.customs.%'
      and event_type not like 'mandala.shipment.bol.%'
)

select
    event_id,
    event_time,
    ingested_at,
    source,
    event_type,
    -- Map to the canonical ShipmentStatus enum.
    case event_type
        when 'mandala.shipment.booked'         then 'booked'
        when 'mandala.shipment.dispatched'     then 'dispatched'
        when 'mandala.shipment.picked_up'      then 'in_transit'
        when 'mandala.shipment.in_transit'     then 'in_transit'
        when 'mandala.shipment.at_border'      then 'at_border'
        when 'mandala.shipment.delivered'      then 'delivered'
        when 'mandala.shipment.cancelled'      then 'cancelled'
        when 'mandala.shipment.eta.updated'    then null
        when 'mandala.shipment.handoff.confirmed' then null
    end                                                              as status,
    subject                                                          as shipment_urn,
    subject_id                                                       as shipment_id,
    subject_scope                                                    as vendor_scope,
    {{ mandala_json_get('payload', 'order_number') }}                as order_number,
    {{ mandala_json_get('payload', 'carrier_scac') }}                as carrier_scac,
    {{ mandala_json_get_timestamp('payload', 'occurred_at') }}       as occurred_at,
    {{ mandala_json_get_number('payload', 'location.lat') }}         as latitude,
    {{ mandala_json_get_number('payload', 'location.lon') }}         as longitude,
    {{ mandala_json_get_timestamp('payload', 'eta') }}               as eta
from events
