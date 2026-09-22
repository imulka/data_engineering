from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC Taxi - Raw Data Inspection")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "data/raw/yellow_tripdata_2013-01.parquet"

print("\n=== READING RAW NYC TAXI DATA ===")

df = spark.read.parquet(INPUT_PATH)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE ROWS ===")
df.show(10, truncate=False)

print("\n=== NUMBER OF RECORDS ===")
row_count = df.count()
print(f"Total rows: {row_count:,}")

print("\n=== NUMBER OF COLUMNS ===")
print(f"Total columns: {len(df.columns)}")

print("\n=== COLUMN NAMES ===")
for column in df.columns:
    print(column)

spark.stop()