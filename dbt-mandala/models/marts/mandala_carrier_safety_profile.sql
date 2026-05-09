{{
  config(
    materialized = 'table',
    tags = ['mandala', 'mart'],
    unique_key = 'dot_number'
  )
}}

-- Carrier safety profile mart from FMCSA SAFER API enrichment.
-- One row per DOT number with live CSA scores, inspection history,
-- and operating authority status. This is the most-used mart for
-- domestic fleets — a live carrier safety scorecard.

with fmcsa_enriched as (
    select * from {{ ref('stg_mandala__events') }}
    where type = 'mandala.carrier.fmcsa.enriched'
),

latest_fmcsa as (
    select
        data:fmcsa:dot_number::string as dot_number,
        data:fmcsa:carrier_name::string as carrier_name,
        data:fmcsa:legal_name::string as legal_name,
        data:fmcsa:dba_name::string as dba_name,
        data:fmcsa:address::string as address,
        data:fmcsa:city::string as city,
        data:fmcsa:state::string as state,
        data:fmcsa:zip::string as zip,
        data:fmcsa:phone::string as phone,
        data:fmcsa:email::string as email,
        data:fmcsa:operating_status::string as operating_status,
        data:fmcsa:authority_type::string as authority_type,
        data:fmcsa:authority_number::string as authority_number,
        data:fmcsa:authority_expiration::string as authority_expiration,
        -- CSA Scores (BASIC categories)
        data:fmcsa:csa_scores:unsafe_driving::number as csa_unsafe_driving,
        data:fmcsa:csa_scores:crash_indicator::number as csa_crash_indicator,
        data:fmcsa:csa_scores:hours_of_service_compliance::number as csa_hos_compliance,
        data:fmcsa:csa_scores:vehicle_maintenance::number as csa_vehicle_maintenance,
        data:fmcsa:csa_scores:controlled_substances_alcohol::number as csa_controlled_substances,
        data:fmcsa:csa_scores:driver_fitness::number as csa_driver_fitness,
        data:fmcsa:csa_scores:hazardous_materials::number as csa_hazardous_materials,
        -- Inspection summary (24-month)
        data:fmcsa:inspections_24mo:vehicle_inspections::number as vehicle_inspections_24mo,
        data:fmcsa:inspections_24mo:driver_inspections::number as driver_inspections_24mo,
        data:fmcsa:inspections_24mo:hazmat_inspections::number as hazmat_inspections_24mo,
        data:fmcsa:inspections_24mo:out_of_service_rate::number as out_of_service_rate_24mo,
        -- Safety rating
        data:fmcsa:safety_rating::string as safety_rating,
        data:fmcsa:safety_rating_date::string as safety_rating_date,
        -- API metadata
        data:fmcsa:last_updated::string as fmcsa_last_updated,
        time as enriched_at,
        row_number() over (
            partition by data:fmcsa:dot_number::string
            order by time desc
        ) as rn
    from fmcsa_enriched
    where data:fmcsa:dot_number is not null
)

select
    dot_number,
    carrier_name,
    legal_name,
    dba_name,
    address,
    city,
    state,
    zip,
    phone,
    email,
    operating_status,
    authority_type,
    authority_number,
    authority_expiration,
    -- CSA Scores
    csa_unsafe_driving,
    csa_crash_indicator,
    csa_hos_compliance,
    csa_vehicle_maintenance,
    csa_controlled_substances,
    csa_driver_fitness,
    csa_hazardous_materials,
    -- Overall CSA score (max of all BASICs)
    greatest(
        coalesce(csa_unsafe_driving, 0),
        coalesce(csa_crash_indicator, 0),
        coalesce(csa_hos_compliance, 0),
        coalesce(csa_vehicle_maintenance, 0),
        coalesce(csa_controlled_substances, 0),
        coalesce(csa_driver_fitness, 0),
        coalesce(csa_hazardous_materials, 0)
    ) as csa_score_max,
    -- Inspection summary
    vehicle_inspections_24mo,
    driver_inspections_24mo,
    hazmat_inspections_24mo,
    out_of_service_rate_24mo,
    -- Safety rating
    safety_rating,
    safety_rating_date,
    -- Metadata
    fmcsa_last_updated,
    enriched_at
from latest_fmcsa
where rn = 1
