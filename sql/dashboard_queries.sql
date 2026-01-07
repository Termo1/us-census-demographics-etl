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
