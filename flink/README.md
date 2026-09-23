# Apache Flink ETL Pipeline — Stock Index Data

This project is a hands-on Apache Flink ETL exercise using historical stock index data from `indexData.csv`.

The goal was to learn the major things Apache Flink does in a real ETL pipeline:

- Read data from a source
- Clean and parse incoming records
- Transform records
- Partition data with `key_by()`
- Perform stateful aggregations
- Assign event time
- Use watermarks
- Create tumbling event-time windows
- Reduce data inside windows
- Write results to a sink

---

## Project Structure

```text
flink/
├── .venv/
├── indexData.csv.zip
├── indexData.csv
├── inspect_data.py
├── flink_step1_source.py
├── flink_step2_parse.py
├── flink_step3_transform.py
├── flink_step4_keyby.py
├── flink_step5_event_time.py
├── flink_step6_windows.py
├── flink_step7.py
└── output_weekly/
```

---

## Dataset

The dataset contains historical market-index data.

Columns:

```text
Index
Date
Open
High
Low
Close
Adj Close
Volume
```

The dataset contains approximately:

```text
112,457 rows
8 columns
```

Example:

```text
NYA,1965-12-31,528.690002,528.690002,528.690002,528.690002,528.690002,0
```

---

# 1. Create the Python Virtual Environment

From inside the Flink project directory:

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

The shell should show something similar to:

```text
(.venv)
```

---

# 2. Install Apache Flink

Install PyFlink:

```bash
python -m pip install apache-flink==2.3.0
```

Installing PyFlink also installs several dependencies, including:

```text
pandas
numpy
pyarrow
apache-beam
py4j
```

Pandas was used only for quick inspection of the CSV. The ETL processing itself is done by Apache Flink.

---

# 3. Extract the Dataset

The original file was:

```text
indexData.csv.zip
```

Extract it using Python:

```bash
python -m zipfile -e indexData.csv.zip .
```

After extraction:

```text
indexData.csv
```

should exist in the current directory.

---

# 4. Inspect the Dataset

`inspect_data.py`:

```python
import pandas as pd

df = pd.read_csv("indexData.csv")

print("Shape:", df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nFirst 5 rows:")
print(df.head())

print("\nData types:")
print(df.dtypes)
```

Run:

```bash
python inspect_data.py
```

Observed schema:

```text
Index         object
Date          object
Open         float64
High         float64
Low          float64
Close        float64
Adj Close    float64
Volume       float64
```

---

# 5. Step 1 — Flink File Source

Script:

```text
flink_step1_source.py
```

The first goal was to make Apache Flink read the CSV as a stream of text records.

The main Flink concepts were:

```text
FileSource
    ↓
StreamFormat
    ↓
env.from_source()
    ↓
DataStream
```

Flink 2.3 uses `FileSource` instead of the older `read_text_file()` API.

The source uses:

```python
FileSource.for_record_stream_format(
    StreamFormat.text_line_format(),
    csv_path
)
```

Run:

```bash
python flink_step1_source.py
```

Example output:

```text
Index,Date,Open,High,Low,Close,Adj Close,Volume
NYA,1965-12-31,528.690002,528.690002,528.690002,528.690002,528.690002,0
NYA,1966-01-03,527.210022,527.210022,527.210022,527.210022,527.210022,0
```

At this stage, every CSV row is still just a raw string.

---

# 6. Step 2 — Parse and Clean the Data

Script:

```text
flink_step2_parse.py
```

The CSV contains a header and some records containing values such as:

```text
null
```

Trying to run:

```python
float("null")
```

causes an error.

The parser therefore:

- Removes the CSV header
- Rejects malformed rows
- Rejects rows containing null numeric values
- Converts numeric strings into Python floats
- Returns typed Flink records

The main operator learned here was:

```text
flat_map()
```

Difference between common Flink transformations:

```text
map()
1 input → exactly 1 output

filter()
1 input → keep or discard

flat_map()
1 input → 0, 1, or many outputs
```

For example:

```text
CSV header
    ↓
flat_map()
    ↓
0 records
```

A valid market record becomes:

```text
('NYA', '1965-12-31', 528.690002, ...)
```

Run:

```bash
python flink_step2_parse.py
```

---

# 7. Step 3 — Transform the Data

Script:

```text
flink_step3_transform.py
```

This step introduced Flink's `map()` operator.

For each market record, the pipeline calculated:

```text
Daily Change % = ((Close - Open) / Open) × 100
```

and:

```text
Daily Range = High - Low
```

Conceptually:

```text
market record
     ↓
   map()
     ↓
enriched market record
```

The transformed record contains:

```text
Index
Date
Close
Volume
Daily Change %
Daily Range
```

Run:

```bash
python flink_step3_transform.py
```

---

# 8. Step 4 — Keyed Processing and Stateful Aggregation

Script:

```text
flink_step4_keyby.py
```

This step introduced one of Flink's most important operations:

```text
key_by()
```

The stream was partitioned using:

```python
record[0]
```

which represents the market index.

Conceptually:

```text
                 ┌── NYA
                 │
DataStream ──────├── IXIC
                 │
                 ├── HSI
                 │
                 └── N225
```

Each key can maintain independent state.

The job then used a reduction function to retain the record with the highest closing price for each index.

Because the CSV is finite, the execution mode was explicitly set to:

```python
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
```

This avoids treating the finite CSV like an endless streaming job.

Run:

```bash
python flink_step4_keyby.py
```

Example result:

```text
Highest closing price by index:

000001.SS  Date: 2007-10-16  Close: 6,092.06
399001.SZ  Date: 2007-10-31  Close: 19,531.15
GDAXI      Date: 2021-05-28  Close: 15,519.98
GSPTSE     Date: 2021-05-28  Close: 19,852.20
HSI        Date: 2018-01-26  Close: 33,154.12
IXIC       Date: 2021-04-26  Close: 14,138.78
J203.JO    Date: 2021-03-11  Close: 68,775.06
KS11       Date: 2021-05-10  Close: 3,249.30
N100       Date: 2021-06-02  Close: 1,263.62
N225       Date: 1989-12-29  Close: 38,915.87
NSEI       Date: 2021-05-31  Close: 15,582.80
NYA        Date: 2021-05-07  Close: 16,590.43
SSMI       Date: 2021-05-28  Close: 11,426.15
TWII       Date: 2021-04-27  Close: 17,595.90
```

---

# 9. Step 5 — Event Time and Watermarks

Script:

```text
flink_step5_event_time.py
```

This step introduced event time.

Instead of asking:

```text
When did Flink process this record?
```

Flink can reason about:

```text
When did this event actually happen?
```

The dataset already contains the event date:

```text
1966-01-05
```

A custom `TimestampAssigner` converts the `Date` field into a Unix timestamp in milliseconds.

Conceptually:

```text
Date field
    ↓
TimestampAssigner
    ↓
Flink event timestamp
    ↓
WatermarkStrategy
```

The exercise used one chronologically ordered index, `NYA`, because `for_monotonous_timestamps()` assumes timestamps move forward within the source partition.

Run:

```bash
python flink_step5_event_time.py
```

Example:

```text
Records with Flink event time assigned:

Index: NYA    Event Date: 1965-12-31  Close: 528.69
Index: NYA    Event Date: 1966-01-03  Close: 527.21
Index: NYA    Event Date: 1966-01-04  Close: 527.84
Index: NYA    Event Date: 1966-01-05  Close: 531.12
Index: NYA    Event Date: 1966-01-06  Close: 532.07
```

Important:

The timestamp is Flink metadata. It does not automatically appear as another field in the printed tuple.

---

# 10. Step 6 — Event-Time Tumbling Windows

Script:

```text
flink_step6_windows.py
```

This step introduced one of the most important Flink concepts:

```text
event-time windows
```

The exercise used:

```python
TumblingEventTimeWindows.of(
    Time.days(7)
)
```

A tumbling window:

- Has a fixed size
- Does not overlap
- Places each event into exactly one window

Conceptually:

```text
NYA events
    ↓
event timestamps
    ↓
key_by("NYA")
    ↓
7-day tumbling windows
    ↓
reduce(highest_close)
    ↓
one maximum record per window
```

The job calculated the highest NYA close in each 7-day event-time window during January 1966.

Run:

```bash
python flink_step6_windows.py
```

Result:

```text
Highest NYA close in each 7-day event-time window:

Date: 1966-01-05   Close: 531.12
Date: 1966-01-11   Close: 534.29
Date: 1966-01-18   Close: 538.94
Date: 1966-01-25   Close: 538.10
Date: 1966-01-27   Close: 537.36
```

The displayed date is the date of the record with the highest close inside the window. It is not necessarily the start or end of the window.

---

# 11. Step 7 — Flink File Sink

Final script:

```text
flink_step7.py
```

The final step completed the ETL pipeline by adding a real Flink sink.

The job writes results using:

```text
FileSink
```

with:

```python
Encoder.simple_string_encoder("UTF-8")
```

The full ETL flow is now:

```text
EXTRACT
indexData.csv
     ↓
FileSource

TRANSFORM
     ↓
clean / parse
     ↓
event time
     ↓
watermarks
     ↓
key_by()
     ↓
7-day tumbling windows
     ↓
reduce()

LOAD
     ↓
FileSink
     ↓
output_weekly/
```

Run:

```bash
python flink_step7.py
```

Expected terminal output:

```text
Running Apache Flink ETL...

ETL complete.
Output written to: /home/isaacm/etlworks/flink/output_weekly
```

---

# 12. Inspect the Flink Output

Flink creates a partition file inside `output_weekly/`.

Find it with:

```bash
find output_weekly -maxdepth 2 -type f -print
```

Example:

```text
output_weekly/2026-09-23--11/part-1875fc09-d047-4589-8bc3-986d384a1650-0
```

Inspect the contents using:

```bash
cat output_weekly/2026-09-23--11/part-1875fc09-d047-4589-8bc3-986d384a1650-0
```

Final output:

```text
NYA,1966-01-05,531.12
NYA,1966-01-11,534.29
NYA,1966-01-18,538.94
NYA,1966-01-25,538.10
NYA,1966-01-27,537.36
```

---

# Important Apache Flink Concepts Learned

## DataStream

A `DataStream` represents records moving through a Flink data-processing pipeline.

```text
Source
  ↓
DataStream
  ↓
Operator
  ↓
DataStream
  ↓
Sink
```

---

## FileSource

Reads records from files into Flink.

Used in this project to read:

```text
indexData.csv
```

---

## flat_map()

Allows one input record to produce:

```text
0
1
or many
```

output records.

We used it for parsing and data-quality filtering.

---

## map()

Transforms one record into one output record.

We used it to calculate derived financial metrics.

---

## key_by()

Partitions a stream by a key.

We keyed records using:

```text
Index
```

This allows Flink to maintain state independently for each market index.

---

## Stateful Processing

After `key_by()`, Flink can maintain values independently for each key.

Example:

```text
NYA  → state
IXIC → state
HSI  → state
N225 → state
```

---

## Event Time

Event time represents when an event actually happened rather than when Flink happened to process it.

Our source was:

```text
Date
```

---

## Watermarks

Watermarks tell Flink how far event time has progressed.

They are important when working with:

- Windows
- Out-of-order events
- Late-arriving events
- Streaming pipelines

---

## Tumbling Windows

Fixed-size, non-overlapping time windows.

This project used:

```text
7-day event-time windows
```

---

## reduce()

Combines multiple records into a smaller result.

We used it to retain the record with the highest closing price.

---

## FileSink

Writes the final Flink stream to persistent storage.

Our result was written to:

```text
output_weekly/
```

---

# Batch vs Streaming

This exercise uses a finite CSV file, so the environment uses:

```python
RuntimeExecutionMode.BATCH
```

Conceptually:

```text
BATCH

finite data
    ↓
process everything
    ↓
finish
```

Flink is especially powerful when the input is an endless stream:

```text
STREAMING

Kafka
  ↓
Flink
  ↓
event time
  ↓
windows
  ↓
aggregations
  ↓
database / dashboard / alerts
```

The same concepts learned here can therefore be applied to streaming systems.

---

# PyFlink Notes From This Exercise

## `read_text_file()` API

Older PyFlink examples may use:

```python
env.read_text_file(...)
```

With the installed Flink version, we used the newer:

```python
FileSource
```

API.

---

## Duration vs Time

For the installed PyFlink window API, this worked:

```python
Time.days(7)
```

instead of:

```python
Duration.of_days(7)
```

because `TumblingEventTimeWindows.of()` expected an object that provides `to_milliseconds()`.

---

## gRPC / Multiplexer Cancellation Messages

Using:

```python
execute_and_collect(limit=5)
```

sometimes produced messages such as:

```text
StatusCode.CANCELLED
Multiplexer hanging up
```

The cleaner approach for bounded jobs was to allow the job to finish naturally and use the collection iterator in a context manager:

```python
with stream.execute_and_collect("job-name") as results:
    records = list(results)
```

---

# How to Restart This Exercise Later

From the project directory:

```bash
source .venv/bin/activate
```

Then run the final ETL:

```bash
python flink_step7.py
```

Inspect generated files:

```bash
find output_weekly -maxdepth 2 -type f -print
```

Then inspect a generated part file with:

```bash
cat <part-file-path>
```

---

# Final Pipeline

```text
indexData.csv
      │
      ▼
┌───────────────┐
│  FileSource   │
└───────┬───────┘
        │
        ▼
 parse / clean
        │
        ▼
 typed DataStream
        │
        ▼
 event timestamps
        │
        ▼
   watermarks
        │
        ▼
   key_by(Index)
        │
        ▼
7-day tumbling window
        │
        ▼
reduce(highest close)
        │
        ▼
┌───────────────┐
│   FileSink    │
└───────┬───────┘
        │
        ▼
 output_weekly/
```

---

# Interview Summary

A concise way to describe this project:

> I built an Apache Flink ETL pipeline in PyFlink using historical financial index data. I used a FileSource to ingest CSV data, cleaned and typed records with DataStream transformations, partitioned records with `key_by`, assigned event timestamps and watermarks, performed stateful 7-day tumbling-window aggregations, and wrote the final results through a FileSink. I also worked with Flink's bounded batch execution mode while using APIs that directly translate to real-time streaming pipelines.

---

## Technologies

```text
Apache Flink 2.3.0
PyFlink
Python 3.12
Apache Beam
Java 21
CSV
```
