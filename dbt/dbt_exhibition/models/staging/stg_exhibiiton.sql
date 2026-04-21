
WITH source AS(
    SELECT * FROM {{source('raw','raw_exhibitions')}}
),

staged AS(
    SELECT
        exhibition_id,
        REGEXP_REPLACE(
            title,
            '^\s*[\[［【〔][^\]］】〕]*[\]］】〕]\s*',
            ''
        ) AS title_cleaned,
        title AS title_raw,
        venue,
        location,
        latitude,
        longitude,

        start_date,
        end_date,
        (end_date - start_date) AS duration_days,

        CASE 
            WHEN is_active=TRUE
            and start_date <=CURRENT_DATE
            and end_date>= CURRENT_DATE THEN  TRUE
            ELSE FALSE
        END AS is_currently_on,

        CASE 
            WHEN end_date < CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_ended,

        CASE 
            WHEN end_date >= CURRENT_DATE THEN (end_date-CURRENT_DATE) 
            ELSE  NULL
        END AS days_remaining,

        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(hours,'<[^>]+>|&[a-z0-9]+;',' ','g'),
                '\s{2,}',' ','g'
            )
        ) AS hours,
        
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(notice,'<[^>]+>|&[a-z0-9]+;',' ','g'),
                '\s{2,}',' ','g'
            )
        ) AS notice,

        age_limit,
        category,
        genre,

        day_rank,
        week_rank,
        month_rank,
        rank AS rank_text,

        (
            SELECT min((elem->>'price')::INTEGER)
            FROM jsonb_array_elements(prices_raw) AS elem 
            WHERE (elem ->> 'price')::INTEGER > 0 
        ) AS price_min,

        (
            SELECT max((elem ->> 'price')::INTEGER)
            FROM jsonb_array_elements(prices_raw) AS elem
            WHERE (elem ->> 'price')::INTEGER > 0
        ) AS price_max,

        (
            SELECT COUNT(*)
            FROM jsonb_array_elements(prices_raw) AS elem
            WHERE (elem ->> 'price')::INTEGER > 0
        ) AS price_options_count,

        CASE 
            WHEN prices_raw IS NOT NULL
            and EXISTS(
                SELECT 1 
                FROM jsonb_array_elements(prices_raw) AS elem
                WHERE elem ->> 'type' ILIKE '%할인%'
            ) THEN  TRUE
            ELSE  FALSE
        END AS has_discount,

        prices_raw,
        image_url,
        detail_url,
        is_active,
        crawled_at,
        updated_at

    FROM source
)
SELECT * FROM staged