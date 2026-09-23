import csv
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat


def parse_market_record(line):
    row = next(csv.reader([line]))

    # Skip header
    if row[0].lstrip("\ufeff") == "Index":
        return []

    # Skip malformed rows
    if len(row) != 8:
        return []

    # Skip rows containing null numeric values
    numeric_values = row[2:8]

    if any(value.strip().lower() in ("null", "") for value in numeric_values):
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
# FLINK ENVIRONMENT
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()
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
# TRANSFORM
# --------------------------------------------------

market_stream = raw_stream.flat_map(
    parse_market_record,
    output_type=Types.TUPLE([
        Types.STRING(),   # Index
        Types.STRING(),   # Date
        Types.DOUBLE(),   # Open
        Types.DOUBLE(),   # High
        Types.DOUBLE(),   # Low
        Types.DOUBLE(),   # Close
        Types.DOUBLE(),   # Adj Close
        Types.DOUBLE(),   # Volume
    ])
)


# --------------------------------------------------
# INSPECT
# --------------------------------------------------

for record in market_stream.execute_and_collect(limit=5):
    print(record)