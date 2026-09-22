from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = (
    SparkSession.builder
    .appName("NYC Taxi - Null Check")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(
    "data/raw/yellow_tripdata_2013-01.parquet"
)

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show(vertical=True)

spark.stop()