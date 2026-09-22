from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Structure Profiling")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT_FILE = (
    "data/raw/"
    "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2026_7.csv"
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_FILE)
)

print("\n=== DATASET SIZE ===")
print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

print("\n=== SPARK SCHEMA ===")
df.printSchema()

spark.stop()