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
