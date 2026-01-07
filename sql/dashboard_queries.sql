-- ============================================
-- Dashboard Visualization Queries
-- US Census Demographics Analysis
-- ============================================

USE SCHEMA census_staging;

-- ============================================
-- Visualization 1: Top 10 States by Population
-- ============================================
SELECT
    s.state_name,
    SUM(f.total_population) AS total_population,
    COUNT(f.cbg_id) AS num_block_groups,
    ROUND(AVG(f.median_age), 1) AS avg_median_age
FROM fact_demographics f
JOIN dim_state s ON f.state_id = s.state_id
GROUP BY s.state_name
ORDER BY total_population DESC
LIMIT 10;

-- ============================================
-- Visualization 2: Income Distribution
-- ============================================
SELECT
    ib.bracket_label,
    ib.income_bracket_id,
    COUNT(f.cbg_id) AS num_block_groups,
    SUM(f.total_population) AS total_population
FROM fact_demographics f
JOIN dim_income_bracket ib ON f.income_bracket_id = ib.income_bracket_id
GROUP BY ib.bracket_label, ib.income_bracket_id
ORDER BY ib.income_bracket_id;

-- ============================================
-- Visualization 3: Average Age by State
-- ============================================
SELECT
    s.state_name,
    ROUND(AVG(f.median_age), 1) AS avg_median_age,
    MIN(f.median_age) AS min_age,
    MAX(f.median_age) AS max_age
FROM fact_demographics f
JOIN dim_state s ON f.state_id = s.state_id
GROUP BY s.state_name
ORDER BY avg_median_age DESC;

-- ============================================
-- Visualization 4: Income vs Population Scatter
-- ============================================
SELECT
    s.state_name,
    SUM(f.total_population) AS total_population,
    ROUND(AVG(f.median_household_income), 0) AS avg_median_income
FROM fact_demographics f
JOIN dim_state s ON f.state_id = s.state_id
GROUP BY s.state_name
ORDER BY total_population DESC;

-- ============================================
-- Visualization 5: Top 10 Counties by Income
-- ============================================
SELECT
    c.county_name,
    s.state_abbr,
    ROUND(AVG(f.median_household_income), 0) AS avg_median_income,
    SUM(f.total_population) AS total_population,
    COUNT(f.cbg_id) AS num_block_groups
FROM fact_demographics f
JOIN dim_county c ON f.county_id = c.county_id
JOIN dim_state s ON c.state_id = s.state_id
WHERE f.median_household_income IS NOT NULL
GROUP BY c.county_name, s.state_abbr
HAVING COUNT(f.cbg_id) >= 10
ORDER BY avg_median_income DESC
LIMIT 10;
