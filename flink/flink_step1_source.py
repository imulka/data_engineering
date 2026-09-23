from pathlib import Path

from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat


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

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "index-file-source"
)

for row in stream.execute_and_collect(limit=5):
    print(row)