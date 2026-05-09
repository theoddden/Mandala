{{ config(materialized='ephemeral') }}

-- Unifies milestones, customs entries, and geofence crossings into one
-- ordered timeline per shipment_urn. Geofence crossings only flow in if
-- the truck has been linked to a shipment via a handoff event in the
-- bridge (when no link exists, the cross-border alert fires instead).

with milestones as (
    select
        shipment_urn,
        event_id,
        event_type,
        event_time,
        status                          as status,
        cast(null as string)            as customs_status,
        order_number                    as detail_a,
        carrier_scac                    as detail_b
    from {{ ref('stg_mandala__shipment_milestones') }}
),

customs as (
    select
        shipment_urn,
        event_id,
        event_type,
        event_time,
        cast(null as string)            as status,
        customs_status                  as customs_status,
        authority                       as detail_a,
        hold_reason                     as detail_b
    from {{ ref('stg_mandala__customs_entries') }}
)

select * from milestones
union all
select * from customs
