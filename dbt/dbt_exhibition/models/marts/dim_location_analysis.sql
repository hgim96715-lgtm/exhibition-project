WITH ex AS(
    SELECT * FROM {{ref ('stg_exhibitions')}}
    WHERE is_active= TRUE
)

SELECT
    COALESCE(location, '기타') AS location,
    COUNT(*) AS total_count,
    COUNT(*) FILTER (
        WHERE
            is_currently_on = TRUE
    ) AS current_count,
    COUNT(*) FILTER (
        WHERE
            has_discount = TRUE
    ) AS discount_count,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0),
        1
    ) AS share_pct,
    ROUND(
        AVG(price_min) FILTER (
            WHERE
                price_min > 0
        ),
        0
    ) AS avg_price_min,
    MIN(price_min) FILTER (
        WHERE
            price_min > 0
    ) AS min_price,
    MAX(price_max) FILTER (
        WHERE
            price_max > 0
    ) AS max_price,
    ROUND(AVG(duration_days), 0) AS avg_duration_days
FROM ex
GROUP BY
    COALESCE(location, '기타')
ORDER BY total_count DESC