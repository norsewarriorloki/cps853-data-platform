
-- ==========================================
-- CPS853: PM2.5 Analytical Results
-- ==========================================

\pset border 2
\pset null '(null)'
\timing on 

-- Question 1: Which states recorded the highest
-- average PM2.5 concentrations in 2024?

\echo
\echo '=========================================='
\echo 'Q1: Highest Annual State PM2.5 - 2024'
\echo '=========================================='

SELECT
    s.state_name,
    a.year,
    ROUND(a.avg_pm25::numeric, 2) AS avg_pm25,
    a.measurement_count
FROM state_annual_pm25 a
JOIN dim_state s
    ON a.state_code = s.state_code
WHERE a.year = 2024
ORDER BY a.avg_pm25 DESC
LIMIT 10;


-- Question 2: Which county-days recorded the
-- highest average PM2.5 concentrations?

\echo
\echo '=========================================='
\echo 'Q2: Highest Daily County PM2.5'
\echo '=========================================='

SELECT
    s.state_name,
    c.county_name,
    d.local_date,
    ROUND(d.avg_pm25::numeric, 2) AS avg_pm25,
    d.measurement_count
FROM county_daily_pm25 d
JOIN dim_state s
    ON d.state_code = s.state_code
JOIN dim_county c
    ON d.state_code = c.state_code
   AND d.county_code = c.county_code
ORDER BY d.avg_pm25 DESC
LIMIT 10;


-- Question 3: Which counties experienced the
-- highest rolling 24-hour average PM2.5?

\echo
\echo '=========================================='
\echo 'Q3: Highest Rolling 24-Hour County PM2.5'
\echo '=========================================='

SELECT
    s.state_name,
    c.county_name,
    r.window_end_utc,
    ROUND(r.max_avg_pm25::numeric, 2) AS max_avg_pm25,
    r.measurement_count
FROM county_rolling_24h_max r
JOIN dim_state s
    ON r.state_code = s.state_code
JOIN dim_county c
    ON r.state_code = c.state_code
   AND r.county_code = c.county_code
ORDER BY r.max_avg_pm25 DESC
LIMIT 10;
