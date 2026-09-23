import csv
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.file_system import (
    FileSource,
    StreamFormat,
)


# --------------------------------------------------
# PARSE CSV
# --------------------------------------------------

def parse_market_record(line):
    row = next(csv.reader([line]))

    if row[0].lstrip("\ufeff") == "Index":
        return []

    if len(row) != 8:
        return []

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
# EVENT-TIME ASSIGNER
# --------------------------------------------------

class MarketTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, record, record_timestamp):

        dt = datetime.strptime(
            record[1],
            "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

        return int(dt.timestamp() * 1000)


# --------------------------------------------------
# FLINK ENVIRONMENT
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)


# --------------------------------------------------
# SOURCE
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
# USE ONE CHRONOLOGICALLY ORDERED INDEX
# --------------------------------------------------

nya_stream = market_stream.filter(
    lambda record:
        record[0] == "NYA"
        and record[1] <= "1966-01-06"
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
# EXECUTE TO NATURAL COMPLETION
# --------------------------------------------------

with timed_stream.execute_and_collect(
    "event-time-demo"
) as results:

    records = list(results)


# --------------------------------------------------
# DISPLAY
# --------------------------------------------------

print("\nRecords with Flink event time assigned:\n")

for record in records:
    print(
        f"Index: {record[0]:5}  "
        f"Event Date: {record[1]}  "
        f"Close: {record[5]:,.2f}"
    )