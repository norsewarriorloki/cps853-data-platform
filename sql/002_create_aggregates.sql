CREATE TABLE IF NOT EXISTS state_annual_pm25 (
    state_code VARCHAR(2) NOT NULL REFERENCES dim_state(state_code),
    year SMALLINT NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (state_code, year)
);

CREATE TABLE IF NOT EXISTS county_daily_pm25 (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    local_date DATE NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (state_code, county_code, local_date),

    FOREIGN KEY (state_code, county_code)
        REFERENCES dim_county(state_code, county_code)
);

CREATE TABLE IF NOT EXISTS county_rolling_24h_max (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    window_end_utc TIMESTAMPTZ NOT NULL,
    max_avg_pm25 DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL,

    PRIMARY KEY (state_code, county_code),

    FOREIGN KEY (state_code, county_code)
        REFERENCES dim_county(state_code, county_code)
);