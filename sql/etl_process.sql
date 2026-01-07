-- ============================================
-- US Census Demographics ETL Process
-- SafeGraph - US Open Census Data
-- ============================================

USE WAREHOUSE COBRA_WH;
USE DATABASE COBRA_DB;
CREATE SCHEMA IF NOT EXISTS census_staging;
USE SCHEMA census_staging;

-- ============================================
-- EXTRACT PHASE: Staging Tables
-- ============================================

-- Staging: Population & Age
CREATE OR REPLACE TABLE stg_population AS
SELECT
    CENSUS_BLOCK_GROUP,
    B01001e1 AS total_population,
    B01001e2 AS male_population,
    B01001e26 AS female_population,
    B01002e1 AS median_age,
    B01003e1 AS total_pop_check
FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B01";

-- Staging: Income
CREATE OR REPLACE TABLE stg_income AS
SELECT
    CENSUS_BLOCK_GROUP,
    B19013e1 AS median_household_income,
    B19001e1 AS total_households,
    B19001e2 AS hh_income_under_10k,
    B19001e3 AS hh_income_10k_15k,
    B19001e4 AS hh_income_15k_20k,
    B19001e5 AS hh_income_20k_25k,
    B19001e6 AS hh_income_25k_30k,
    B19001e7 AS hh_income_30k_35k,
    B19001e8 AS hh_income_35k_40k,
    B19001e9 AS hh_income_40k_45k,
    B19001e10 AS hh_income_45k_50k,
    B19001e11 AS hh_income_50k_60k,
    B19001e12 AS hh_income_60k_75k,
    B19001e13 AS hh_income_75k_100k,
    B19001e14 AS hh_income_100k_125k,
    B19001e15 AS hh_income_125k_150k,
    B19001e16 AS hh_income_150k_200k,
    B19001e17 AS hh_income_200k_plus
FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B19";

-- Staging: FIPS Codes (County/State mapping)
CREATE OR REPLACE TABLE stg_fips AS
SELECT
    STATE,
    STATE_FIPS,
    COUNTY_FIPS,
    COUNTY,
    CLASS_CODE
FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES";

-- Staging: Geographic Data
CREATE OR REPLACE TABLE stg_geo AS
SELECT *
FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_GEOGRAPHIC_DATA";

-- ============================================
-- TRANSFORM PHASE: Dimension Tables
-- ============================================

-- Dimenzia: DIM_STATE (SCD Type 0)
CREATE OR REPLACE TABLE dim_state AS
SELECT
    ROW_NUMBER() OVER (ORDER BY STATE_FIPS) AS state_id,
    STATE_FIPS AS state_fips,
    STATE AS state_abbr,
    CASE STATE
        WHEN 'AL' THEN 'Alabama'
        WHEN 'AK' THEN 'Alaska'
        WHEN 'AZ' THEN 'Arizona'
        WHEN 'AR' THEN 'Arkansas'
        WHEN 'CA' THEN 'California'
        WHEN 'CO' THEN 'Colorado'
        WHEN 'CT' THEN 'Connecticut'
        WHEN 'DE' THEN 'Delaware'
        WHEN 'DC' THEN 'District of Columbia'
        WHEN 'FL' THEN 'Florida'
        WHEN 'GA' THEN 'Georgia'
        WHEN 'HI' THEN 'Hawaii'
        WHEN 'ID' THEN 'Idaho'
        WHEN 'IL' THEN 'Illinois'
        WHEN 'IN' THEN 'Indiana'
        WHEN 'IA' THEN 'Iowa'
        WHEN 'KS' THEN 'Kansas'
        WHEN 'KY' THEN 'Kentucky'
        WHEN 'LA' THEN 'Louisiana'
        WHEN 'ME' THEN 'Maine'
        WHEN 'MD' THEN 'Maryland'
        WHEN 'MA' THEN 'Massachusetts'
        WHEN 'MI' THEN 'Michigan'
        WHEN 'MN' THEN 'Minnesota'
        WHEN 'MS' THEN 'Mississippi'
        WHEN 'MO' THEN 'Missouri'
        WHEN 'MT' THEN 'Montana'
        WHEN 'NE' THEN 'Nebraska'
        WHEN 'NV' THEN 'Nevada'
        WHEN 'NH' THEN 'New Hampshire'
        WHEN 'NJ' THEN 'New Jersey'
        WHEN 'NM' THEN 'New Mexico'
        WHEN 'NY' THEN 'New York'
        WHEN 'NC' THEN 'North Carolina'
        WHEN 'ND' THEN 'North Dakota'
        WHEN 'OH' THEN 'Ohio'
        WHEN 'OK' THEN 'Oklahoma'
        WHEN 'OR' THEN 'Oregon'
        WHEN 'PA' THEN 'Pennsylvania'
        WHEN 'RI' THEN 'Rhode Island'
        WHEN 'SC' THEN 'South Carolina'
        WHEN 'SD' THEN 'South Dakota'
        WHEN 'TN' THEN 'Tennessee'
        WHEN 'TX' THEN 'Texas'
        WHEN 'UT' THEN 'Utah'
        WHEN 'VT' THEN 'Vermont'
        WHEN 'VA' THEN 'Virginia'
        WHEN 'WA' THEN 'Washington'
        WHEN 'WV' THEN 'West Virginia'
        WHEN 'WI' THEN 'Wisconsin'
        WHEN 'WY' THEN 'Wyoming'
        WHEN 'PR' THEN 'Puerto Rico'
        ELSE STATE
    END AS state_name
FROM stg_fips
GROUP BY STATE_FIPS, STATE
ORDER BY STATE_FIPS;

-- Dimenzia: DIM_COUNTY (SCD Type 0)
CREATE OR REPLACE TABLE dim_county AS
SELECT
    ROW_NUMBER() OVER (ORDER BY f.STATE_FIPS, f.COUNTY_FIPS) AS county_id,
    f.STATE_FIPS || f.COUNTY_FIPS AS county_fips_full,
    f.COUNTY_FIPS AS county_fips,
    f.COUNTY AS county_name,
    s.state_id,
    f.CLASS_CODE AS class_code
FROM stg_fips f
JOIN dim_state s ON f.STATE_FIPS = s.state_fips;

-- Dimenzia: DIM_INCOME_BRACKET (SCD Type 0)
CREATE OR REPLACE TABLE dim_income_bracket (
    income_bracket_id INT,
    bracket_code VARCHAR(10),
    income_min INT,
    income_max INT,
    bracket_label VARCHAR(50)
);

INSERT INTO dim_income_bracket VALUES
(1, 'B19001e2', 0, 9999, 'Under $10,000'),
(2, 'B19001e3', 10000, 14999, '$10,000 - $14,999'),
(3, 'B19001e4', 15000, 19999, '$15,000 - $19,999'),
(4, 'B19001e5', 20000, 24999, '$20,000 - $24,999'),
(5, 'B19001e6', 25000, 29999, '$25,000 - $29,999'),
(6, 'B19001e7', 30000, 34999, '$30,000 - $34,999'),
(7, 'B19001e8', 35000, 39999, '$35,000 - $39,999'),
(8, 'B19001e9', 40000, 44999, '$40,000 - $44,999'),
(9, 'B19001e10', 45000, 49999, '$45,000 - $49,999'),
(10, 'B19001e11', 50000, 59999, '$50,000 - $59,999'),
(11, 'B19001e12', 60000, 74999, '$60,000 - $74,999'),
(12, 'B19001e13', 75000, 99999, '$75,000 - $99,999'),
(13, 'B19001e14', 100000, 124999, '$100,000 - $124,999'),
(14, 'B19001e15', 125000, 149999, '$125,000 - $149,999'),
(15, 'B19001e16', 150000, 199999, '$150,000 - $199,999'),
(16, 'B19001e17', 200000, NULL, '$200,000 or more');

-- Dimenzia: DIM_AGE_GROUP (SCD Type 0)
CREATE OR REPLACE TABLE dim_age_group (
    age_group_id INT,
    age_min INT,
    age_max INT,
    age_label VARCHAR(30),
    life_stage VARCHAR(20)
);

INSERT INTO dim_age_group VALUES
(1, 0, 4, 'Under 5', 'Child'),
(2, 5, 9, '5-9', 'Child'),
(3, 10, 14, '10-14', 'Child'),
(4, 15, 17, '15-17', 'Teen'),
(5, 18, 24, '18-24', 'Young Adult'),
(6, 25, 34, '25-34', 'Adult'),
(7, 35, 44, '35-44', 'Adult'),
(8, 45, 54, '45-54', 'Adult'),
(9, 55, 64, '55-64', 'Adult'),
(10, 65, 74, '65-74', 'Senior'),
(11, 75, 84, '75-84', 'Senior'),
(12, 85, NULL, '85+', 'Senior');

-- ============================================
-- LOAD PHASE: Fact Table with Window Functions
-- ============================================

CREATE OR REPLACE TABLE fact_demographics AS
WITH base_data AS (
    SELECT
        p.CENSUS_BLOCK_GROUP AS cbg_id,
        LEFT(p.CENSUS_BLOCK_GROUP, 2) AS state_fips,
        LEFT(p.CENSUS_BLOCK_GROUP, 5) AS county_fips_full,

        -- Population metriky
        p.total_population,
        p.male_population,
        p.female_population,
        p.median_age,

        -- Income metriky
        i.median_household_income,
        i.total_households,

        -- Percentualne vypocty
        CASE WHEN p.total_population > 0
            THEN ROUND(p.male_population * 100.0 / p.total_population, 2)
            ELSE NULL END AS pct_male,
        CASE WHEN p.total_population > 0
            THEN ROUND(p.female_population * 100.0 / p.total_population, 2)
            ELSE NULL END AS pct_female,

        -- Income bracket determination
        CASE
            WHEN i.median_household_income < 10000 THEN 1
            WHEN i.median_household_income < 15000 THEN 2
            WHEN i.median_household_income < 20000 THEN 3
            WHEN i.median_household_income < 25000 THEN 4
            WHEN i.median_household_income < 30000 THEN 5
            WHEN i.median_household_income < 35000 THEN 6
            WHEN i.median_household_income < 40000 THEN 7
            WHEN i.median_household_income < 45000 THEN 8
            WHEN i.median_household_income < 50000 THEN 9
            WHEN i.median_household_income < 60000 THEN 10
            WHEN i.median_household_income < 75000 THEN 11
            WHEN i.median_household_income < 100000 THEN 12
            WHEN i.median_household_income < 125000 THEN 13
            WHEN i.median_household_income < 150000 THEN 14
            WHEN i.median_household_income < 200000 THEN 15
            WHEN i.median_household_income >= 200000 THEN 16
            ELSE NULL
        END AS income_bracket_id

    FROM stg_population p
    LEFT JOIN stg_income i ON p.CENSUS_BLOCK_GROUP = i.CENSUS_BLOCK_GROUP
    WHERE p.total_population > 0
)

SELECT
    ROW_NUMBER() OVER (ORDER BY cbg_id) AS fact_id,
    b.cbg_id,
    s.state_id,
    c.county_id,
    b.income_bracket_id,

    -- Metriky
    b.total_population,
    b.male_population,
    b.female_population,
    b.median_age,
    b.median_household_income,
    b.total_households,
    b.pct_male,
    b.pct_female,

    -- ═══════════════════════════════════════════════════════════
    -- WINDOW FUNCTIONS (POVINNA CAST!)
    -- ═══════════════════════════════════════════════════════════

    -- 1. RANK() - Poradie podla populacie v ramci statu
    RANK() OVER (
        PARTITION BY s.state_id
        ORDER BY b.total_population DESC
    ) AS population_rank_in_state,

    -- 2. RANK() - Poradie podla prijmu v ramci statu
    RANK() OVER (
        PARTITION BY s.state_id
        ORDER BY b.median_household_income DESC NULLS LAST
    ) AS income_rank_in_state,

    -- 3. PERCENT_RANK() - Percentil populacie celonarodne
    ROUND(PERCENT_RANK() OVER (
        ORDER BY b.total_population
    ), 4) AS population_percentile,

    -- 4. PERCENT_RANK() - Percentil prijmu celonarodne
    ROUND(PERCENT_RANK() OVER (
        ORDER BY b.median_household_income NULLS FIRST
    ), 4) AS income_percentile,

    -- 5. SUM() OVER - Kumulativna populacia v ramci statu
    SUM(b.total_population) OVER (
        PARTITION BY s.state_id
        ORDER BY b.cbg_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_pop_in_state,

    -- 6. AVG() OVER - Priemer prijmu v county
    ROUND(AVG(b.median_household_income) OVER (
        PARTITION BY c.county_id
    ), 0) AS avg_income_in_county,

    -- ═══════════════════════════════════════════════════════════

    -- Metadata
    2019 AS data_year,
    CURRENT_TIMESTAMP() AS load_timestamp

FROM base_data b
JOIN dim_state s ON b.state_fips = s.state_fips
JOIN dim_county c ON b.county_fips_full = c.county_fips_full;

-- Verifikacia
SELECT COUNT(*) AS total_facts FROM fact_demographics;
SELECT * FROM fact_demographics LIMIT 10;
