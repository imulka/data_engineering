import csv
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat


def parse_market_record(line):
    row = next(csv.reader([line]))

    if row[0].lstrip("\ufeff") == "Index":
        return []

    if len(row) != 8:
        return []

    if any(value.strip().lower() in ("null", "") for value in row[2:8]):
        return []

    return [(
        row[0],
        row[1],
        float(row[2]),
        float(row[3]),
        float(row[4]),
        float(row[5]),
        float(row[6]),
        float(row[7]),
    )]


def calculate_metrics(record):
    index, date, open_price, high, low, close, adj_close, volume = record

    daily_change_pct = ((close - open_price) / open_price) * 100
    daily_range = high - low

    return (
        index,
        date,
        close,
        volume,
        daily_change_pct,
        daily_range,
    )


def keep_highest_close(record1, record2):
    if record1[2] >= record2[2]:
        return record1

    return record2


# --------------------------------------------------
# FLINK ENVIRONMENT
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

# Our CSV is finite, so use batch semantics.
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

env.set_parallelism(1)


# --------------------------------------------------
# EXTRACT
# --------------------------------------------------

csv_path = Path("indexData.csv").resolve().as_uri()

source = (
    FileSource
    .for_record_stream_format(
        StreamFormat.text_line_format(),
        csv_path
    )
    .build()
)

raw_stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "index-file-source"
)


# --------------------------------------------------
# CLEAN / PARSE
# --------------------------------------------------

market_stream = raw_stream.flat_map(
    parse_market_record,
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
    ])
)


# --------------------------------------------------
# TRANSFORM
# --------------------------------------------------

metrics_stream = market_stream.map(
    calculate_metrics,
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
    ])
)


# --------------------------------------------------
# KEY BY INDEX
# --------------------------------------------------

keyed_stream = metrics_stream.key_by(
    lambda record: record[0],
    key_type=Types.STRING()
)


# --------------------------------------------------
# REDUCE
# One final highest-close record per index
# --------------------------------------------------

max_close_stream = keyed_stream.reduce(
    keep_highest_close
)


# --------------------------------------------------
# EXECUTE
# --------------------------------------------------

with max_close_stream.execute_and_collect(
    "highest-close-by-index"
) as results:

    final_results = list(results)


# --------------------------------------------------
# DISPLAY
# --------------------------------------------------

print("\nHighest closing price by index:\n")

for record in sorted(final_results, key=lambda x: x[0]):
    print(
        f"{record[0]:10} "
        f"Date: {record[1]}  "
        f"Close: {record[2]:,.2f}"
    )