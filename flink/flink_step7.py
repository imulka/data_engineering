import csv
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import Encoder

from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)

from pyflink.datastream.connectors.file_system import (
    FileSource,
    StreamFormat,
    FileSink,
)

from pyflink.datastream.window import TumblingEventTimeWindows


# --------------------------------------------------
# PARSE / CLEAN
# --------------------------------------------------

def parse_market_record(line):
    row = next(csv.reader([line]))

    # Skip header
    if row[0].lstrip("\ufeff") == "Index":
        return []

    # Skip malformed rows
    if len(row) != 8:
        return []

    # Skip null numeric values
    if any(value.strip().lower() in ("null", "") for value in row[2:8]):
        return []

    return [(
        row[0],          # Index
        row[1],          # Date
        float(row[2]),   # Open
        float(row[3]),   # High
        float(row[4]),   # Low
        float(row[5]),   # Close
        float(row[6]),   # Adj Close
        float(row[7]),   # Volume
    )]


# --------------------------------------------------
# EVENT TIME
# --------------------------------------------------

class MarketTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, record, record_timestamp):

        dt = datetime.strptime(
            record[1],
            "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

        return int(dt.timestamp() * 1000)


# --------------------------------------------------
# REDUCE
# Keep highest closing-price record in each window
# --------------------------------------------------

def highest_close(record1, record2):

    if record1[5] >= record2[5]:
        return record1

    return record2


# --------------------------------------------------
# FLINK ENVIRONMENT
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)


# --------------------------------------------------
# EXTRACT — FILE SOURCE
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
# TRANSFORM — CLEAN / PARSE
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
# FILTER — NYA JANUARY 1966
# --------------------------------------------------

nya_stream = market_stream.filter(
    lambda record:
        record[0] == "NYA"
        and "1966-01-01" <= record[1] <= "1966-01-31"
)


# --------------------------------------------------
# ASSIGN EVENT TIME
# --------------------------------------------------

watermark_strategy = (
    WatermarkStrategy
    .for_monotonous_timestamps()
    .with_timestamp_assigner(
        MarketTimestampAssigner()
    )
)

timed_stream = nya_stream.assign_timestamps_and_watermarks(
    watermark_strategy
)


# --------------------------------------------------
# KEY + 7-DAY WINDOW + REDUCE
# --------------------------------------------------

weekly_max_stream = (
    timed_stream
    .key_by(
        lambda record: record[0],
        key_type=Types.STRING()
    )
    .window(
        TumblingEventTimeWindows.of(
            Time.days(7)
        )
    )
    .reduce(
        highest_close
    )
)


# --------------------------------------------------
# PREPARE OUTPUT
# --------------------------------------------------

output_stream = weekly_max_stream.map(
    lambda record: (
        f"{record[0]},"
        f"{record[1]},"
        f"{record[5]:.2f}"
    ),
    output_type=Types.STRING()
)


# --------------------------------------------------
# LOAD — FILE SINK
# --------------------------------------------------

output_path = str(
    Path("output_weekly").resolve()
)

sink = (
    FileSink
    .for_row_format(
        output_path,
        Encoder.simple_string_encoder("UTF-8")
    )
    .build()
)

output_stream.sink_to(sink)


# --------------------------------------------------
# RUN ETL
# --------------------------------------------------

print("Running Apache Flink ETL...")

env.execute("NYA Weekly ETL")

print("\nETL complete.")
print(f"Output written to: {output_path}")