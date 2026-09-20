# Building Materials dbt ETL/ELT Pipeline

This project demonstrates a local analytics engineering pipeline using **dbt Core** and **DuckDB**.

## Pipeline

```text
building_materials_transactions.csv
              |
              v
           dbt seed
              |
              v
building_materials_transactions
              |
              v
stg_building_materials_transactions
              |
        +-----+-----+
        |           |
        v           v
fct_revenue_     fct_monthly_
by_region        product_revenue
```

The source dataset contains approximately **340,000 building-material transaction records**.

## Technology

- Python 3.12
- dbt Core
- dbt-duckdb
- DuckDB
- SQL

## Project Structure

```text
building_materials/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_building_materials_transactions.sql
│   │   └── stg_building_materials_transactions.yml
│   └── marts/
│       ├── fct_revenue_by_region.sql
│       └── fct_monthly_product_revenue.sql
├── seeds/
│   └── building_materials_transactions.csv
├── scripts/
│   ├── inspect_raw_data.py
│   ├── inspect_revenue_by_region.py
│   └── inspect_monthly_product_revenue.py
└── tests/
    ├── test_positive_units.sql
    ├── test_positive_unit_price.sql
    ├── test_non_negative_revenue.sql
    └── test_revenue_calculation.sql
```

## Start the Project

From a new terminal:

```bash
cd /home/isaacm/etlworks
source .venv/bin/activate
cd dbt/building_materials
```

Verify dbt:

```bash
dbt --version
```

Verify the project and database connection:

```bash
dbt debug
```

## Load the Raw CSV

```bash
dbt seed
```

This loads:

```text
seeds/building_materials_transactions.csv
```

into DuckDB as:

```text
building_materials_transactions
```

## Build the Pipeline

Run the entire dbt pipeline:

```bash
dbt build
```

This loads the seed, builds the staging model, runs data-quality tests, and builds the analytics marts.

Expected successful result:

```text
PASS=14
WARN=0
ERROR=0
```

## Run Models Only

```bash
dbt run
```

Run only the staging model:

```bash
dbt run --select stg_building_materials_transactions
```

Run only the regional revenue mart:

```bash
dbt run --select fct_revenue_by_region
```

Run only the monthly product mart:

```bash
dbt run --select fct_monthly_product_revenue
```

## Run Tests

```bash
dbt test
```

The pipeline tests:

- Required fields are not null
- Units are greater than zero
- Unit prices are greater than zero
- Revenue is not negative
- Revenue approximately equals units multiplied by unit price

## Inspect Results

Raw data:

```bash
python scripts/inspect_raw_data.py
```

Regional revenue:

```bash
python scripts/inspect_revenue_by_region.py
```

Monthly product revenue:

```bash
python scripts/inspect_monthly_product_revenue.py
```

## Generate dbt Documentation

```bash
dbt docs generate
```

Start the documentation server:

```bash
dbt docs serve --port 8082
```

Open:

```text
http://localhost:8082
```

The dbt documentation site displays models, columns, tests, and the lineage DAG.

Stop the documentation server with:

```text
Ctrl+C
```

## Important dbt Concepts Demonstrated

### `ref()`

Models reference one another using:

```sql
{{ ref('stg_building_materials_transactions') }}
```

This allows dbt to determine dependencies and construct the project DAG automatically.

### Staging Layer

The staging model cleans and standardizes raw data before downstream analytics models consume it.

### Mart Layer

The marts contain business-ready aggregated datasets suitable for dashboards and analytics.

### Data Tests

dbt validates both structural requirements and business rules before downstream models are considered trustworthy.

### `dbt build`

The main command for this exercise is:

```bash
dbt build
```

It executes the project according to its dependency graph and runs associated tests.

## Pipeline Summary

```text
CSV
 |
 v
DuckDB Raw Table
 |
 v
dbt Staging Model
 |
 v
Data Quality Tests
 |
 +---------------------+
 |                     |
 v                     v
Revenue by Region    Monthly Product Revenue
```
