import csv
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
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


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

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

metrics_stream = market_stream.map(
    calculate_metrics,
    output_type=Types.TUPLE([
        Types.STRING(),   # Index
        Types.STRING(),   # Date
        Types.DOUBLE(),   # Close
        Types.DOUBLE(),   # Volume
        Types.DOUBLE(),   # Daily change %
        Types.DOUBLE(),   # Daily range
    ])
)

for record in metrics_stream.execute_and_collect(limit=5):
    print(record)