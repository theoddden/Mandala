{{ config(materialized='ephemeral') }}

-- Sessionise truck positions into journeys. A new journey starts whenever
-- the gap from the previous position exceeds 30 minutes, or when the
-- engine_state transitions through 'off'.

with positions as (
    select
        truck_urn,
        event_time,
        latitude,
        longitude,
        speed_mps,
        odometer_km,
        fuel_pct,
        engine_state,
        lag(event_time)    over (partition by truck_urn order by event_time) as prev_event_time,
        lag(engine_state)  over (partition by truck_urn order by event_time) as prev_engine_state,
        lag(odometer_km)   over (partition by truck_urn order by event_time) as prev_odometer_km
    from {{ ref('stg_mandala__truck_positions') }}
),

flagged as (
    select
        *,
        case
            when prev_event_time is null then 1
            when {{ dbt.datediff('prev_event_time', 'event_time', 'minute') }} > 30 then 1
            when prev_engine_state = 'off' and engine_state <> 'off' then 1
            else 0
        end as is_journey_start
    from positions
),

assigned as (
    select
        *,
        sum(is_journey_start) over (
            partition by truck_urn
            order by event_time
            rows between unbounded preceding and current row
        ) as journey_seq
    from flagged
)

select
    truck_urn,
    journey_seq,
    min(event_time)              as journey_start,
    max(event_time)              as journey_end,
    count(*)                     as position_count,
    max(odometer_km) - min(odometer_km) as distance_km,
    avg(speed_mps)               as avg_speed_mps,
    min(fuel_pct)                as min_fuel_pct,
    max(fuel_pct)                as max_fuel_pct
from assigned
group by truck_urn, journey_seq
