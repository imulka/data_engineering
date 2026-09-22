# Delta Lake + Apache Iceberg ETL Pipeline

## Overview

This project demonstrates a practical ETL and lakehouse workflow using:

- Apache Spark / PySpark
- Delta Lake
- Apache Iceberg
- U.S. DOT/BTS Airline On-Time Performance data

The pipeline ingests raw monthly flight data, profiles and validates the source, applies transformations, loads the curated data into both Delta Lake and Apache Iceberg, and demonstrates lakehouse features such as updates, MERGE operations, schema evolution, transaction history, snapshots, and time travel.

---

## Dataset

Source:

**U.S. Department of Transportation — Bureau of Transportation Statistics (BTS)**  
On-Time Performance dataset for **July 2026**.

Raw CSV size:

- approximately 274 MB
- 631,970 flight records
- 110 source columns

The source contains flight schedule, airline, airport, delay, cancellation, diversion, taxi, distance, and operational information.

---

## Project Structure

```text
delta_iceberg/
├── data/
│   └── raw/
│       ├── On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2026_7.csv
│       └── readme.html
├── scripts/
├── warehouse/
│   ├── delta/
│   │   └── flights/
│   └── iceberg/
│       └── bts/
│           └── flights/
├── .venv/
└── README.md
```

---

## Environment

The project uses an isolated Python virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the Spark and Delta dependencies:

```bash
pip install pyspark==4.0.4 delta-spark==4.0.0
```

Apache Iceberg is loaded through the Spark runtime package:

```text
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0
```

Environment used:

```text
Python 3.12.3
PySpark 4.0.4
Delta Lake 4.0.0
Apache Iceberg 1.11.0
OpenJDK 21
```

---

## ETL Workflow

### 1. Extract

The BTS ZIP archive was downloaded and extracted into `data/raw/`.

The raw flight dataset contains **631,970 rows**.

---

### 2. Profile and Validate

The dataset was inspected before transformation.

Important findings:

- 109 meaningful source fields
- one empty trailing CSV column (`_c109`)
- 16,530 cancelled flights
- 3,402 diverted flights
- no duplicate flights using the selected natural key
- no zero or negative flight distances
- no invalid cancellation/diversion flags
- no missing origin or destination airports

Many NULL values were determined to be valid business NULLs rather than bad data.

For example, `CancellationCode` is NULL when a flight was not cancelled.

---

## Natural Flight Key

The following fields were tested together and produced no duplicate records:

```text
FlightDate
Reporting_Airline
Flight_Number_Reporting_Airline
Origin
Dest
CRSDepTime
```

This key was later used for MERGE operations.

---

## Transformations

The main transformations included:

- removed the empty `_c109` column
- converted `Cancelled` and `Diverted` from double values to integers
- created a readable `flight_status` field
- added a derived `route` field

Flight status logic:

```text
CANCELLED
DIVERTED
DELAYED
COMPLETED
CORRECTED
```

Example route transformation:

```text
Origin = ATL
Dest   = RIC

route = ATL-RIC
```

---

## Delta Lake

The cleaned Spark DataFrame was written to:

```text
warehouse/delta/flights
```

Delta stores the underlying data as Parquet files and maintains table transaction metadata in:

```text
_delta_log/
```

### Delta Operations Demonstrated

#### Initial Write

```text
Version 0 -> WRITE
```

#### Update

Flights arriving at least 15 minutes late were changed from `COMPLETED` to `DELAYED`.

```text
Version 1 -> UPDATE
```

#### MERGE

A specific existing flight record was matched using the natural flight key and changed to:

```text
flight_status = CORRECTED
```

```text
Version 2 -> MERGE
```

#### Schema Evolution

A new field was added:

```text
route STRING
```

Delta history:

```text
Version 3 -> ADD COLUMNS
Version 4 -> UPDATE
```

---

## Apache Iceberg

The same transformed data was written to:

```text
warehouse/iceberg/bts/flights
```

Iceberg separates data and metadata:

```text
flights/
├── data/
└── metadata/
```

Iceberg tracks table state using snapshots rather than Delta-style sequential version numbers.

### Iceberg Operations Demonstrated

- initial table creation
- UPDATE
- MERGE INTO
- schema evolution
- snapshot history
- time travel

Example snapshot IDs created during the exercise:

```text
2906684518609543475
4938703400957907113
8729310637427247583
2807385407416515085
```

Iceberg used copy-on-write behavior for row-level updates, so update and merge operations appeared as `overwrite` snapshot operations.

---

## Time Travel

Both table formats were queried before and after the delayed-flight update.

### Before Update

```text
COMPLETED   612,038
CANCELLED    16,530
DIVERTED      3,402
```

### Current State

```text
COMPLETED   438,577
DELAYED     173,460
CANCELLED    16,530
DIVERTED      3,402
CORRECTED         1
```

### Delta

Historical data was queried using a Delta table version:

```text
versionAsOf = 0
```

### Iceberg

Historical data was queried using an Iceberg snapshot:

```sql
VERSION AS OF <snapshot_id>
```

---

## Final Validation

Both Delta and Iceberg ended with identical results:

```text
Rows:           631,970
Columns:        111
Missing routes: 0
```

Final flight status counts:

| Flight Status | Count |
|---|---:|
| COMPLETED | 438,577 |
| DELAYED | 173,460 |
| CANCELLED | 16,530 |
| DIVERTED | 3,402 |
| CORRECTED | 1 |
| **Total** | **631,970** |

---

## Delta Lake vs. Apache Iceberg

| Feature | Delta Lake | Apache Iceberg |
|---|---|---|
| Storage | Parquet | Parquet |
| Metadata | `_delta_log` | `metadata/` |
| Historical state | Version numbers | Snapshot IDs |
| UPDATE | Yes | Yes |
| MERGE | Yes | Yes |
| Time travel | Yes | Yes |
| Schema evolution | Yes | Yes |
| ACID transactions | Yes | Yes |
| Spark support | Yes | Yes |

---

## Key Takeaways

This exercise demonstrated that Delta Lake and Apache Iceberg are not replacements for Spark.

Spark performs the distributed data processing, while Delta Lake and Iceberg provide a transactional table layer over data stored in formats such as Parquet.

The biggest practical difference from ordinary Parquet files is that these table formats maintain metadata about table state, enabling reliable:

- updates
- deletes
- merges
- schema changes
- historical queries
- transactional data pipelines

The project also reinforced an important ETL principle:

> NULL values are not automatically bad data. Their meaning must be evaluated using the business context of the dataset.

---

## Running the Project

Activate the environment:

```bash
cd ~/etlworks/delta_iceberg
source .venv/bin/activate
```

The scripts in `scripts/` cover data inspection, profiling, transformation, Delta Lake operations, Apache Iceberg operations, time travel, MERGE, schema evolution, and final validation.

---

## Technologies

- Python
- PySpark
- Apache Spark
- Delta Lake
- Apache Iceberg
- Parquet
- U.S. DOT / BTS flight data

---

## Purpose

This project is part of a hands-on data engineering practice series focused on building one practical ETL/data-platform stack at a time.
