CREATE DATABASE airflow;
-- CREATE USER airflow WITH PASSWORD 'airflow';
-- GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
-- ALTER DATABASE airflow OWNER TO airflow;

CREATE TABLE IF NOT EXISTS raw_exhibitions (
    id SERIAL PRIMARY KEY,
    exhibition_id VARCHAR(50) UNIQUE NOT NULL, -- goodsCode
    title VARCHAR(500) NOT NULL,
    venue VARCHAR(300),
    location VARCHAR(50),
    address VARCHAR(500),
    longitude NUMERIC(10, 7),
    latitude NUMERIC(10, 7),
    start_date DATE,
    end_date DATE,
    hours TEXT,
    age_limit VARCHAR(100),
    category VARCHAR(100),
    genre VARCHAR(100),
    day_rank INTEGER,
    week_rank INTEGER,
    month_rank INTEGER,
    rank VARCHAR(100),
    image_url VARCHAR(500),
    detail_url VARCHAR(500),
    notice TEXT,
    prices_raw JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw_exhibitions IS 'Summary API 원본 적재 테이블';

COMMENT ON COLUMN raw_exhibitions.prices_raw IS '/v1/goods/{goodCode}/prices/group 원문 JSONB';

COMMENT ON COLUMN raw_exhibitions.day_rank IS 'dayRank(숫자)';

-- 가격 상세 테이블

CREATE TABLE IF NOT EXISTS raw_exhibition_prices (
    id SERIAL PRIMARY KEY,
    exhibition_id VARCHAR(50) NOT NULL REFERENCES raw_exhibitions (exhibition_id) ON DELETE CASCADE,
    seat_grade VARCHAR(20),
    seat_grade_name VARCHAR(100),
    price_grade VARCHAR(20),
    price_grade_name VARCHAR(300),
    price_type_code VARCHAR(20),
    price_type_name VARCHAR(100),
    sales_price INTEGER,
    origin_price INTEGER,
    discount_rate NUMERIC(5, 2),
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        exhibition_id,
        seat_grade,
        price_grade,
        price_type_code
    )
);

-- 통계 API booking

CREATE TABLE IF NOT EXISTS raw_exhibition_stats (
    id SERIAL PRIMARY KEY,
    exhibition_id VARCHAR(50) NOT NULL REFERENCES raw_exhibitions (exhibition_id) ON DELETE CASCADE,
    age10_rate NUMERIC(5, 2),
    age20_rate NUMERIC(5, 2),
    age30_rate NUMERIC(5, 2),
    age40_rate NUMERIC(5, 2),
    age50_rate NUMERIC(5, 2),
    male_rate NUMERIC(5, 2),
    female_rate NUMERIC(5, 2),
    stats_raw JSONB,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (exhibition_id, snapshot_date)
);

COMMENT ON
TABLE raw_exhibition_stats IS '/v1/statistics/booking ageGender 원본 적재';

CREATE TABLE IF NOT EXISTS raw_exhibition_history (
    id SERIAL PRIMARY KEY,
    exhibition_id VARCHAR(50) NOT NULL REFERENCES raw_exhibitions (exhibition_id) ON DELETE CASCADE,
    day_rank INTEGER,
    week_rank INTEGER,
    month_rank INTEGER,
    prices_raw JSONB,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (exhibition_id, snapshot_date)
);

COMMENT ON TABLE raw_exhibition_history IS '순위/가격 변동 추이 추적용 일별 스냅샷';

--INDEX

CREATE INDEX IF NOT EXISTS idx_ex_location ON raw_exhibitions (location);

CREATE INDEX IF NOT EXISTS idx_ex_category ON raw_exhibitions (category);

CREATE INDEX IF NOT EXISTS idx_ex_active_period ON raw_exhibitions (
    is_active,
    start_date,
    end_date
);

CREATE INDEX IF NOT EXISTS idx_ex_week_rank ON raw_exhibitions (week_rank)
WHERE
    week_rank IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_prices_exhibition ON raw_exhibition_prices (exhibition_id);

CREATE INDEX IF NOT EXISTS idx_prices_seat_grade ON raw_exhibition_prices (seat_grade_name);

CREATE INDEX IF NOT EXISTS idx_stats_exhibition ON raw_exhibition_stats (exhibition_id);

CREATE INDEX IF NOT EXISTS idx_history_exhibition ON raw_exhibition_history (exhibition_id);

CREATE INDEX IF NOT EXISTS idx_history_date ON raw_exhibition_history (snapshot_date);

CREATE OR REPLACE VIEW v_active_exhibitions AS
SELECT *
FROM raw_exhibitions
WHERE
    is_active = TRUE
    AND start_date <= CURRENT_DATE
    AND end_date >= CURRENT_DATE;

COMMENT ON VIEW v_active_exhibitions IS '현재 진행 중인 전시 정보 조회용 뷰';