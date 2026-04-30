WITH base AS(
    SELECT * FROM {{ ref('stg_exhibitions')}}
    WHERE is_currently_on = TRUE
),
stats AS(
    SELECT * FROM {{ ref('stg_exhibition_stats')}}
)

SELECT
    b.exhibition_id,
    b.title_cleaned AS title,
    b.venue,
    b.location,
    b.address,
    b.latitude,
    b.longitude,
    b.start_date,
    b.end_date,
    b.days_remaining,
    b.hours,
    b.age_limit,
    b.category,
    b.genre,
    b.week_rank,
    b.day_rank,
    b.month_rank,
    b.rank_text,
    b.price_min,
    b.price_max,
    b.price_options_count,
    b.has_discount,
    s.age10_rate,
    s.age20_rate,
    s.age30_rate,
    s.age40_rate,
    s.age50_rate,
    s.male_rate,
    s.female_rate,
    s.top_age_group,
    s.gender_dominant,
    b.image_url,
    b.detail_url,
    b.notice,
    b.crawled_at
FROM base b
    LEFT JOIN stats s ON b.exhibition_id = s.exhibition_id
ORDER BY b.days_remaining ASC, b.week_rank ASC