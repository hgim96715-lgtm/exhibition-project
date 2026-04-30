
WITH ex AS(
    SELECT 
        exhibition_id,
        title_cleaned AS title,
        location,
        category,
        week_rank,
        price_min,
        is_currently_on,
        has_discount
    FROM {{ref('stg_exhibitions')}}
    WHERE is_active=TRUE
),

stats AS(
    SELECT * FROM {{ref ('stg_exhibition_stats')}}
)

SELECT
    s.exhibition_id,
    e.title,
    e.location,
    e.category,
    e.week_rank,
    e.price_min,
    e.is_currently_on,
    e.has_discount,
    s.age10_rate,
    s.age20_rate,
    s.age30_rate,
    s.age40_rate,
    s.age50_rate,
    s.male_rate,
    s.female_rate,
    s.top_age_group,
    s.gender_dominant,
    s.snapshot_date
FROM stats s
    JOIN ex e ON s.exhibition_id = e.exhibition_id
ORDER BY e.week_rank ASC NULLS LAST