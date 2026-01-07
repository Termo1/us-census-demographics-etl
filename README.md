# US Census Demographics - ETL Project

Tento projekt analyzuje demograficke data z US Census pomocou ETL procesu v Snowflake. Data pochádzaju z datasetu **SafeGraph - US Open Census Data & Neighborhood Insights** dostupneho cez Snowflake Marketplace.

## 1. Uvod a popis zdrojovych dat

Cielom projektu je analyzovat demograficke udaje obyvatelstva USA na urovni Census Block Groups (CBG). Projekt vyuziva data z American Community Survey (ACS) 2019 5-Year Estimates.

### Zdrojova databaza
- **Snowflake Database**: `US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET`
- **Schema**: `PUBLIC`
- **Warehouse**: `COBRA_WH`

### Zdrojove tabulky

| Tabulka | Popis | Pocet riadkov |
|---------|-------|---------------|
| `2019_CBG_B01` | Pohlavie a vek | 220,333 |
| `2019_CBG_B19` | Prijem domacnosti | 220,333 |
| `2019_METADATA_CBG_FIPS_CODES` | FIPS kody statov a counties | 3,233 |
| `2019_METADATA_CBG_GEOGRAPHIC_DATA` | Geograficke data (lat/lng) | 220,333 |

### Struktura CENSUS_BLOCK_GROUP ID

```
CENSUS_BLOCK_GROUP = "010010201001"
                      ││││││││││││
                      ││││││││└┴┴┴── Block Group (4 digits)
                      ││││││└┴────── Tract (6 digits)
                      │││└┴┴──────── County FIPS (3 digits)
                      └┴──────────── State FIPS (2 digits)
```

### Klucove Census kody

| Kod | Popis |
|-----|-------|
| B01001e1 | Total Population |
| B01002e1 | Median Age |
| B01001e2 | Male Population |
| B01001e26 | Female Population |
| B19013e1 | Median Household Income |
| B19001e2-e17 | Income brackets |
