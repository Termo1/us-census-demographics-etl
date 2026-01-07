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
