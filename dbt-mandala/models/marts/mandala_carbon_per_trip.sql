{{ config(materialized='table', tags=['mandala', 'mart']) }}

-- Measured CO2 per truck journey from fuel-consumption telemetry.
-- Defaults to diesel (≈ 2.68 kg CO2 per litre, US EPA factor) when fuel
-- type is unknown. Operators with mixed fleets should override the
-- emission factors via dbt vars.

{% set fuel_factor_diesel = var('mandala', {}).get('co2_kg_per_litre_diesel', 2.68) %}
{% set fuel_factor_gasoline = var('mandala', {}).get('co2_kg_per_litre_gasoline', 2.31) %}
{% set diesel_l_per_100km = var('mandala', {}).get('default_diesel_l_per_100km', 35.0) %}
{% set ev_kwh_per_km = var('mandala', {}).get('default_ev_kwh_per_km', 1.4) %}
{% set grid_g_co2_per_kwh = var('mandala', {}).get('grid_g_co2_per_kwh', 387) %}

with journeys as (
    select * from {{ ref('int_mandala__truck_journeys') }}
),

trucks as (
    select * from {{ ref('mandala_trucks_current') }}
),

joined as (
    select
        j.*,
        coalesce(t.fuel_type, 'diesel')  as fuel_type
    from journeys j
    left join trucks t using (truck_urn)
)

select
    truck_urn,
    journey_seq,
    journey_start,
    journey_end,
    distance_km,
    avg_speed_mps,
    fuel_type,
    case
        when fuel_type = 'electric' then
            (distance_km * {{ ev_kwh_per_km }}) * {{ grid_g_co2_per_kwh }} / 1000.0
        when fuel_type = 'gasoline' then
            (distance_km * {{ diesel_l_per_100km }} / 100.0) * {{ fuel_factor_gasoline }}
        else
            (distance_km * {{ diesel_l_per_100km }} / 100.0) * {{ fuel_factor_diesel }}
    end as co2_kg_estimated
from joined
where distance_km is not null and distance_km > 0
