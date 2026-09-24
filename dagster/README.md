# Dagster ETL Practice Guide

This project is a hands-on Dagster ETL exercise using:

- Python 3.12
- Dagster
- Pandas
- CSV input/output
- Dagster assets
- Dagster asset checks
- Dagster lineage
- Local Dagster web UI

The source dataset is:

```text
Social_media_impact_on_life.csv
```

The pipeline performs:

```text
CSV
 ↓
raw_social_media_data
 ↓
clean_social_media_data
 ↓
social_media_metrics
 ↓
export_pipeline_outputs
```

The cleaning asset also has three data-quality checks:

```text
✓ no_missing_values
✓ student_id_unique
✓ gpa_valid_range
```

---

## 1. Open the project

From a new terminal:

```bash
cd ~/etlworks/dagster
```

Confirm the files:

```bash
ls
```

You should see files similar to:

```text
dagster_pipeline.py
inspect_data.py
requirements.txt
Social_media_impact_on_life.csv
output/
```

---

## 2. Create the virtual environment

You only need to do this if `.venv` does not already exist.

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Your terminal prompt should show:

```text
(.venv)
```

---

## 3. Install the dependencies

If this is a fresh environment:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Verify Dagster:

```bash
dagster --version
```

The exercise was originally built with Dagster 1.13.24.

---

## 4. Inspect the source dataset

Before running Dagster, you can inspect the CSV:

```bash
python inspect_data.py
```

The original dataset contains:

```text
4500 rows
16 columns
```

The two columns that originally contained missing values were:

```text
Perceived_Stress_Score       46 missing
Academic_Performance_GPA     85 missing
```

There were no duplicate rows.

---

## 5. Set persistent Dagster storage

Create a local Dagster home if it does not already exist:

```bash
mkdir -p .dagster_home
```

Set the environment variable:

```bash
export DAGSTER_HOME="$(pwd)/.dagster_home"
```

This prevents Dagster from creating a temporary storage directory that disappears when the server stops.

You need to run the `export DAGSTER_HOME=...` command again whenever you open a new terminal session unless you add it to your shell configuration.

---

## 6. Start Dagster

Make sure the virtual environment is active:

```bash
source .venv/bin/activate
```

Set Dagster home:

```bash
export DAGSTER_HOME="$(pwd)/.dagster_home"
```

Then launch Dagster:

```bash
dagster dev -f dagster_pipeline.py
```

If your installed Dagster version recommends the newer command, you can use:

```bash
dg dev -f dagster_pipeline.py
```

Dagster should start on:

```text
http://127.0.0.1:3000
```

Open that address in your browser.

Keep this terminal running while you use Dagster.

---

## 7. Materialize the pipeline

In the Dagster UI, open the asset lineage.

You should see:

```text
raw_social_media_data
          ↓
clean_social_media_data
          ↓
social_media_metrics
          ↓
export_pipeline_outputs
```

For a full re-practice run, materialize the assets in this order:

### Step 1 — Raw data

Materialize:

```text
raw_social_media_data
```

Expected log messages:

```text
Loaded 4500 rows
Columns: 16
```

### Step 2 — Clean data

Materialize:

```text
clean_social_media_data
```

Expected results:

```text
Missing stress scores before cleaning: 46
Missing GPA values before cleaning: 85
Rows after cleaning: 4500
Remaining null values: 0
```

The missing numeric values are filled using the median of their respective columns.

### Step 3 — Run data-quality checks

The clean asset includes:

```text
no_missing_values
student_id_unique
gpa_valid_range
```

Expected result:

```text
3 / 3 checks passed
```

### Step 4 — Analytics

Materialize:

```text
social_media_metrics
```

This groups the data by:

```text
Primary_Platform
```

and calculates:

```text
student_count
avg_daily_usage_hours
avg_sleep_hours
avg_stress_score
avg_mental_health_index
avg_gpa
```

### Step 5 — Load/export

Materialize:

```text
export_pipeline_outputs
```

This writes:

```text
output/clean_social_media_data.csv
output/social_media_metrics.csv
```

---

## 8. Verify the output files

From a second terminal:

```bash
cd ~/etlworks/dagster
```

Check the output folder:

```bash
ls -lh output/
```

You should see:

```text
clean_social_media_data.csv
social_media_metrics.csv
```

Inspect the aggregated metrics:

```bash
cat output/social_media_metrics.csv
```

The original run produced:

```text
Primary_Platform,student_count,avg_daily_usage_hours,avg_sleep_hours,avg_stress_score,avg_mental_health_index,avg_gpa
Instagram,1453,5.26,6.71,13.41,80.01,3.43
LinkedIn,96,5.27,6.75,14.05,79.39,3.43
Reddit,229,5.31,6.64,13.67,79.28,3.43
Snapchat,472,5.46,6.67,13.96,78.99,3.43
TikTok,1226,5.33,6.68,13.59,79.85,3.43
X (Twitter),206,5.14,6.81,12.47,80.81,3.43
YouTube,818,5.25,6.72,13.45,80.16,3.42
```

---

## 9. Stop Dagster

In the terminal running Dagster:

```text
Ctrl+C
```

Then deactivate the Python environment:

```bash
deactivate
```

---

## 10. Quick restart commands

For the next practice session, the shortest restart sequence is:

```bash
cd ~/etlworks/dagster
source .venv/bin/activate
export DAGSTER_HOME="$(pwd)/.dagster_home"
dagster dev -f dagster_pipeline.py
```

Then open:

```text
http://127.0.0.1:3000
```

---

## 11. If a downstream asset fails with FileNotFoundError

If you see an error similar to:

```text
FileNotFoundError:
.../storage/raw_social_media_data
```

it means Dagster does not have the upstream asset materialized in its current storage.

Materialize:

```text
raw_social_media_data
```

first, then materialize:

```text
clean_social_media_data
```

Using a persistent `.dagster_home` greatly reduces this problem between restarts.

---

## 12. Useful Git commands

From the repository root:

```bash
cd ~/etlworks
```

Check changes:

```bash
git status --short
```

Stage only the Dagster project:

```bash
git add dagster/
```

Commit:

```bash
git commit -m "Update Dagster ETL pipeline exercise"
```

Push:

```bash
git push
```

Avoid `git add .` if unrelated files elsewhere in the repository have changes.

---

## Interview summary

A concise way to describe this project:

> I built an asset-based ETL pipeline in Dagster that extracts a CSV dataset, cleans missing values, applies Dagster asset-level data-quality checks, creates platform-level analytics, and exports processed datasets. I used Dagster's asset dependency graph and web UI to monitor materializations, lineage, metadata, and validation results.

Key Dagster concepts demonstrated:

- Assets
- Asset dependencies
- Materialization
- Asset lineage
- Asset checks
- Metadata
- IO management
- Persistent Dagster storage
- Web UI monitoring
- ETL orchestration
