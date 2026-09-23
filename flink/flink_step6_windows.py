import csv
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.file_system import (
    FileSource,
    StreamFormat,
)
from pyflink.datastream.window import TumblingEventTimeWindows


# --------------------------------------------------
# PARSE
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
        row[0],
        row[1],
        float(row[2]),
        float(row[3]),
        float(row[4]),
        float(row[5]),
        float(row[6]),
        float(row[7]),
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
# REDUCE FUNCTION
# Keep record with highest Close
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
# CLEAN
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
# FILTER
# NYA — January 1966
# --------------------------------------------------

nya_stream = market_stream.filter(
    lambda record:
        record[0] == "NYA"
        and "1966-01-01" <= record[1] <= "1966-01-31"
)


# --------------------------------------------------
# ASSIGN EVENT TIME + WATERMARK
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
# KEY + 7-DAY EVENT-TIME WINDOW
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
# EXECUTE
# --------------------------------------------------

with weekly_max_stream.execute_and_collect(
    "weekly-window-demo"
) as results:

    weekly_results = list(results)


# --------------------------------------------------
# DISPLAY
# --------------------------------------------------

print("\nHighest NYA close in each 7-day event-time window:\n")

for record in weekly_results:

    print(
        f"Date: {record[1]}   "
        f"Close: {record[5]:,.2f}"
    )