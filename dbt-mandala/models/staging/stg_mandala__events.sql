{{
  config(
    materialized = 'view',
    tags = ['mandala', 'staging']
  )
}}

-- The single base relation: one row per Mandala CloudEvent ingested into the
-- warehouse. Every other staging model derives from this view, so JSON access
-- happens exactly once per event_id.

with src as (
    select * from {{ mandala_raw_relation() }}
),

renamed as (
    select
        cast(event_id as string)               as event_id,
        cast(event_type as string)             as event_type,
        cast(source as string)                 as source,
        cast(subject as string)                as subject,
        cast(event_time as timestamp)          as event_time,
        cast(ingested_at as timestamp)         as ingested_at,
        cast(schema_version as string)         as schema_version,
        payload                                as payload,
        {{ mandala_urn_entity('subject') }}    as subject_entity,
        {{ mandala_urn_scope('subject') }}     as subject_scope,
        {{ mandala_urn_id('subject') }}        as subject_id
    from src
)

select * from renamed
