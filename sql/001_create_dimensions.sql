CREATE TABLE IF NOT EXISTS dim_state (
    state_code VARCHAR(2) PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_county (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    county_name VARCHAR(150) NOT NULL,

    PRIMARY KEY (state_code, county_code),

    FOREIGN KEY (state_code)
        REFERENCES dim_state(state_code)
);