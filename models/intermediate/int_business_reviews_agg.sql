{{ config(materialized='view') }}

with business as (
    select
        business_id,
        name as business_name,
        city,
        state,
        avg_rating as business_rating,
        review_count as total_reviews,
        is_open
    from {{ ref('stg_yelp_business') }}
),

reviews as (
    select
        business_id,
        review_rating,
        review_date,
        total_reaction_count,
        row_number() over (partition by business_id order by review_date desc) as review_recency_rank
    from {{ ref('stg_yelp_review') }}
),

aggregated as (
    select
        b.business_id,
        b.business_name,
        b.city,
        b.state,
        b.business_rating,
        b.total_reviews,
        b.is_open,
        count(r.review_rating) as review_count_calculated,
        round(avg(r.review_rating)::numeric, 2) as avg_review_rating,
        min(r.review_rating) as min_review_rating,
        max(r.review_rating) as max_review_rating,
        max(r.review_date) as latest_review_date,
        round(avg(r.total_reaction_count)::numeric, 2) as avg_reactions_per_review
    from business b
    left join reviews r on b.business_id = r.business_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from aggregated
