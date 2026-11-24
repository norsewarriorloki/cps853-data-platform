-- ============================================
-- Spark Gold: Annual PM2.5 by State
-- ============================================

CREATE TABLE IF NOT EXISTS stg_state_annual_pm25 (
    state_code VARCHAR(2) NOT NULL,
    year INTEGER NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (state_code, year)
);


-- ============================================
-- Spark Gold: Daily PM2.5 by County
-- ============================================

CREATE TABLE IF NOT EXISTS stg_county_daily_pm25 (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    local_date DATE NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (state_code, county_code, local_date)
);


-- ============================================
-- Spark Streaming: Rolling 24-Hour PM2.5
-- ============================================

CREATE TABLE IF NOT EXISTS stg_county_rolling_24h (
    source_topic TEXT NOT NULL,
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    window_end_utc TIMESTAMP NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (
        source_topic,
        state_code,
        county_code,
        window_end_utc
    )
);