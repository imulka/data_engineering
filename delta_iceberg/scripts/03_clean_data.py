from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC Taxi - Data Cleaning")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(
    "data/raw/yellow_tripdata_2013-01.parquet"
)

print("Columns before cleaning:", len(df.columns))

clean_df = df.drop(
    "congestion_surcharge",
    "airport_fee"
)

print("Columns after cleaning:", len(clean_df.columns))

clean_df.printSchema()

spark.stop()