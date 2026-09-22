from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Duplicate Check")
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

# Candidate natural key for a scheduled flight
key_columns = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime"
]

duplicates = (
    df.groupBy(key_columns)
    .count()
    .filter(col("count") > 1)
)

duplicate_groups = duplicates.count()

duplicate_rows = (
    duplicates
    .selectExpr("sum(count - 1) AS duplicate_rows")
    .collect()[0]["duplicate_rows"]
)

print("\n=== DUPLICATE CHECK ===")
print(f"Duplicate flight groups: {duplicate_groups:,}")
print(f"Extra duplicate rows:    {duplicate_rows or 0:,}")

if duplicate_groups > 0:
    print("\nSample duplicates:")
    duplicates.orderBy(col("count").desc()).show(20, truncate=False)

spark.stop()