{{ config(materialized='view') }}

with source as (
    select
        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars as avg_rating,
        review_count,
        is_open,
        attributes,
        categories,
        hours
    from {{ source('yelp_raw', 'raw_yelp_business') }}
    where business_id is not null
)

select * from source
