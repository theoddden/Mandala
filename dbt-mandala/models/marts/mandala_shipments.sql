{{
  config(
    materialized = 'table',
    tags = ['mandala', 'mart'],
    unique_key = 'shipment_urn'
  )
}}

-- One row per shipment. The "single pane of glass" mart that fleet
-- operators demo to executives. Latest known status, customs state, ETA,
-- and a JSON timeline blob.

with milestones as (
    select * from {{ ref('stg_mandala__shipment_milestones') }}
),

customs as (
    select * from {{ ref('stg_mandala__customs_entries') }}
),

latest_milestone as (
    select
        shipment_urn,
        shipment_id,
        vendor_scope,
        status,
        carrier_scac,
        eta,
        latitude,
        longitude,
        event_time as last_milestone_at,
        row_number() over (
            partition by shipment_urn
            order by event_time desc
        ) as rn
    from milestones
    where status is not null
),

latest_customs as (
    select
        shipment_urn,
        customs_status,
        authority,
        entry_number,
        hold_reason,
        broker_name,
        importer_name,
        filed_at,
        released_at,
        event_time as last_customs_at,
        row_number() over (
            partition by shipment_urn
            order by event_time desc
        ) as rn
    from customs
),

eta_latest as (
    select
        shipment_urn,
        eta,
        row_number() over (
            partition by shipment_urn
            order by event_time desc
        ) as rn
    from milestones
    where eta is not null
)

select
    lm.shipment_urn,
    lm.shipment_id,
    lm.vendor_scope,
    lm.status,
    coalesce(lc.customs_status, 'not_filed') as customs_status,
    lm.carrier_scac,
    coalesce(el.eta, lm.eta)                 as eta,
    lm.latitude                              as last_latitude,
    lm.longitude                             as last_longitude,
    lc.authority                             as customs_authority,
    lc.entry_number                          as customs_entry_number,
    lc.hold_reason                           as customs_hold_reason,
    lc.broker_name,
    lc.importer_name,
    lc.filed_at                              as customs_filed_at,
    lc.released_at                           as customs_released_at,
    lm.last_milestone_at,
    lc.last_customs_at,
    greatest(
        coalesce(lm.last_milestone_at, lc.last_customs_at),
        coalesce(lc.last_customs_at, lm.last_milestone_at)
    )                                        as last_event_at
from latest_milestone lm
left join latest_customs lc
       on lm.shipment_urn = lc.shipment_urn
      and lc.rn = 1
left join eta_latest el
       on lm.shipment_urn = el.shipment_urn
      and el.rn = 1
where lm.rn = 1
