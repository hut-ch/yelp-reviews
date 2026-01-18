{{ config(materialized='view') }}

with source as (
    select
        review_id,
        user_id,
        business_id,
        stars as review_rating,
        date as review_date,
        text as review_text,
        useful,
        funny,
        cool,
        (useful + funny + cool) as total_reaction_count
    from {{ source('yelp_raw', 'raw_yelp_review') }}
    where review_id is not null
        and business_id is not null
        and user_id is not null
)

select * from source
