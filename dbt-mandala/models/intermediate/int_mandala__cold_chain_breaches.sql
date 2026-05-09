{{ config(materialized='ephemeral') }}

-- Group consecutive breach events into windows. A new window opens whenever
-- a breach reading appears more than 5 minutes after the previous reading
-- on the same truck/sensor pair, or whenever a non-breach reading
-- intervenes.

with readings as (
    select
        truck_urn,
        sensor_id,
        event_time,
        temperature_c,
        is_breach,
        lag(event_time) over (partition by truck_urn, sensor_id order by event_time) as prev_event_time,
        lag(is_breach)  over (partition by truck_urn, sensor_id order by event_time) as prev_is_breach
    from {{ ref('stg_mandala__cold_chain_readings') }}
),

flagged as (
    select
        *,
        case
            when is_breach
             and (prev_is_breach is null
                  or not prev_is_breach
                  or {{ dbt.datediff('prev_event_time', 'event_time', 'minute') }} > 5)
            then 1
            else 0
        end as is_window_start
    from readings
    where is_breach
),

assigned as (
    select
        *,
        sum(is_window_start) over (
            partition by truck_urn, sensor_id
            order by event_time
            rows between unbounded preceding and current row
        ) as window_seq
    from flagged
)

select
    truck_urn,
    sensor_id,
    window_seq,
    min(event_time) as breach_start,
    max(event_time) as breach_end,
    min(temperature_c) as min_temperature_c,
    max(temperature_c) as max_temperature_c,
    count(*)        as reading_count
from assigned
group by truck_urn, sensor_id, window_seq
