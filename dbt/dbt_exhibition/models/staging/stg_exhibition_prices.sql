
WITH source AS(
    SELECT * FROM {{source('raw','raw_exhibition_prices')}}
),

staged AS (
    SELECT
        exhibition_id,
        seat_grade_name,
        price_grade_name,
        price_type_name,
        price_type_code,
        sales_price,

-- origin_price 보정 로직:
--   기본가       → 할인 없음 → origin_price = sales_price
--   기본가할인   → origin_price > 0 이면 사용
--                  origin_price = 0 이고 discount_rate > 0 이면 역산
--                  둘 다 0 이면 NULL (알 수 없음)
CASE 
            WHEN price_type_name NOT ILIKE '%할인%' THEN  sales_price
            WHEN origin_price >0 THEN origin_price
            WHEN discount_rate >0 AND sales_price>0 THEN round(sales_price/(1-discount_rate/100.0))
            ELSE NULL
        END AS origin_price_calc,

        discount_rate,
        CASE 
            WHEN price_type_name ILIKE '%할인%' THEN TRUE
            ELSE FALSE
        END AS is_discount,

        CASE 
            WHEN price_type_name NOT ILIKE '%할인%' THEN  0 
            WHEN origin_price>0 AND origin_price>sales_price THEN origin_price - sales_price
            WHEN discount_rate > 0  AND sales_price >0 THEN round(sales_price/(1-discount_rate/100.0) - sales_price)
            ELSE  NULL
        END AS discount_amount,

        CASE 
            WHEN price_type_name NOT ILIKE '%할인%' THEN 0 
            WHEN discount_rate > 0 THEN discount_rate
            WHEN origin_price > 0 AND origin_price > sales_price THEN round(100.0 *(origin_price - sales_price)/origin_price,1)  
            ELSE  NULL
        END AS discount_rate_calc,

        crawled_at
    
    FROM source
    WHERE sales_price>0

)
SELECT * FROM staged