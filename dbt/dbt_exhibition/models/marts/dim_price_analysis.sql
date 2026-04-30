
WITH ex AS (
    SELECT *
    FROM {{ ref('stg_exhibitions')}}
    WHERE is_active = TRUE
),

price_agg AS (
    SELECT
        exhibition_id,
        MIN(sales_price) AS price_min_actual,
        MAX(sales_price) AS price_max_actual,
        MAX(discount_rate_calc) AS max_discount_rate
    FROM {{ ref('stg_exhibition_prices')}}
    GROUP BY exhibition_id
),

combined AS (
    SELECT
        e.exhibition_id,
        e.title_cleaned AS title,
        e.location,
        e.category,
        e.is_currently_on,
        e.week_rank,

        CASE 
            WHEN e.price_min IS NULL THEN '가격 미정'
            WHEN e.price_min < 10000 THEN '1만원 미만'
            WHEN e.price_min < 20000 THEN '1만~2만원'
            WHEN e.price_min < 30000 THEN '2만~3만원'
            WHEN e.price_min < 50000 THEN '3만~5만원'
            ELSE '5만원 이상'
        END AS price_range,

        COALESCE(p.price_min_actual, e.price_min) AS price_min_final,
        COALESCE(p.price_max_actual, e.price_max) AS price_max_final,

        e.has_discount,
        p.max_discount_rate

    FROM ex e
    LEFT JOIN price_agg p 
        ON e.exhibition_id = p.exhibition_id
)

SELECT * FROM combined ORDER BY price_min_final ASC NULLS LAST