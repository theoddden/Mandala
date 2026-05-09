{{ config(materialized='table', tags=['mandala', 'mart']) }}

-- Cold-chain breach windows from int_mandala__cold_chain_breaches, enriched
-- with the *declared* min/max temp from the truck's currently linked shipment
-- (when known). Out-of-spec rows are the regulatory liability surface.

with breaches as (
    select * from {{ ref('int_mandala__cold_chain_breaches') }}
)

select
    b.truck_urn,
    b.sensor_id,
    b.window_seq,
    b.breach_start,
    b.breach_end,
    {{ dbt.datediff('breach_start', 'breach_end', 'minute') }} as duration_minutes,
    b.min_temperature_c,
    b.max_temperature_c,
    b.reading_count,
    -- Hooks for the customer to join their own master shipment table
    -- against truck_urn → shipment_urn association. Left null in v0.1.
    cast(null as string)  as shipment_urn,
    cast(null as float)   as declared_min_c,
    cast(null as float)   as declared_max_c,
    case
        when max_temperature_c is null then 'unknown'
        else 'breach_recorded'
    end as compliance_status
from breaches b
