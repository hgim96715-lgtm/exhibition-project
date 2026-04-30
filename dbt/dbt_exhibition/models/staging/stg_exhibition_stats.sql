
WITH source AS (
    SELECT * FROM {{source('raw','raw_exhibition_stats')}}
),

staged AS(
    SELECT 
        exhibition_id,
        snapshot_date,

        {{ get_top_category({
            '10대': 'age10_rate',
            '20대': 'age20_rate',
            '30대': 'age30_rate',
            '40대': 'age40_rate',
            '50대': 'age50_rate'
        }) }} AS top_age_group,

        COALESCE(age10_rate,0) AS age10_rate,
        COALESCE(age20_rate,0) AS age20_rate,
        COALESCE(age30_rate,0) AS age30_rate,
        COALESCE(age40_rate,0) AS age40_rate,
        COALESCE(age50_rate,0) AS age50_rate,

        male_rate,
        female_rate,

        {{ get_top_category({
            "남성": "male_rate",
            "여성": "female_rate"
        }) }} AS gender_dominant,

        crawled_at

    FROM source
)

SELECT * FROM staged