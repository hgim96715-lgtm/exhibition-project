
WITH source AS(
    SELECT * FROM {{source('raw','raw_exhibitions')}}
),

html_stripped AS(
    SELECT *,
        REGEXP_REPLACE(hours, '<[^>]+>|&[a-z0-9#]+;', ' ', 'g') AS hours_no_html,
        REGEXP_REPLACE(notice, '<[^>]+>|&[a-z0-9#]+;', ' ', 'g') AS notice_no_html
    FROM source
),

hours_extracted AS(
    SELECT *,
        CASE WHEN hours_no_html ~* '관람시간|운영\s*시간|관람일정'
        THEN REGEXP_REPLACE(
            hours_no_html,
            '^.*(관람시간|운영\s*시간|관람일정)\s*[\[【〔〕】\]：:）]*\s*',
            '', 'i'
        )
        ELSE hours_no_html
        END AS hours_extracted
    FROM html_stripped 
),

hours_bullet_cleaned AS (
    SELECT *,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    hours_extracted,
                    '(^|\s+)[-·*•,]+\s*', -- 1. 시작점 또는 공백 뒤의 기호들(- , * ·)과 그 뒤 공백까지 싹 잡기
                    ' ', 
                    'g'
                ),
                '\s{2,}', -- 2. 기호 지우고 남은 중복 공백 정리
                ' ', 
                'g'
            )
        ) AS hours_bullet_cleaned
    FROM hours_extracted
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
        address,
        latitude,
        longitude,

        start_date,
        end_date,
        (end_date - start_date) AS duration_days,

        CASE 
            WHEN is_active=TRUE
            and start_date <= CURRENT_DATE
            and end_date >= CURRENT_DATE THEN TRUE  
            ELSE FALSE
        END AS is_currently_on,

        CASE 
            WHEN end_date < CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_ended,

        CASE 
            WHEN end_date >= CURRENT_DATE THEN (end_date - CURRENT_DATE)
            ELSE NULL
        END AS days_remaining,

        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    -- STEP 3: ※ 이후 제거 (※ 부터 문자열 끝까지 포함)
                    REGEXP_REPLACE(
                        hours_bullet_cleaned,
                        '\s*※[\s\S]*',
                        ''
                    ),
                    '[▶◆◇■□●○★☆△▲▽▼◀►▷\[【〔〕】\]）（｛｝［］]',
                    ' ',
                    'g'
                ),
                '\s{2,}',
                ' ',
                'g'
            )
        ) as hours,

TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    notice_no_html,
                    '(?i)^About\s*[｜|]\s*Exhibition', -- ① "About｜Exhibition" (대소문자 무시) 제거
                    ' ', 'g'
                ),
                '［[^］]+］', -- ② "［북촌］", "［주의사항］" 등 괄호와 그 안의 내용 삭제
                ' ', 'g'
            ),
            '[※▶◆◇■□●○★☆△▲▽▼◀►▷*·,：:]+', -- ③ 기호 및 콜론(전각/반각) 제거
            ' ', 'g'
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
            WHERE (elem ->> 'price'):: INTEGER > 0
        ) AS price_max,

        (
            SELECT COUNT(*)
            FROM jsonb_array_elements(prices_raw) AS elem
            WHERE (elem ->> 'price')::INTEGER >0
        ) AS price_options_count,

        CASE 
            WHEN prices_raw IS NOT NULL
            and EXISTS(
                SELECT 1
                FROM jsonb_array_elements(prices_raw) AS elem
                WHERE elem ->> 'type' ILIKE '%할인%'
            ) THEN TRUE
            ELSE FALSE
        END AS has_discount,

        prices_raw,
        image_url,
        detail_url,
        is_active,
        crawled_at,
        updated_at
    
    FROM hours_bullet_cleaned
)

SELECT * FROM staged