from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Core Transformation")
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

# 1. Remove trailing blank CSV column
clean_df = df.drop("_c109")

# 2. Normalize binary flags
clean_df = (
    clean_df
    .withColumn("Cancelled", col("Cancelled").cast("integer"))
    .withColumn("Diverted", col("Diverted").cast("integer"))
)

# 3. Create a human-readable flight status
clean_df = clean_df.withColumn(
    "flight_status",
    when(col("Cancelled") == 1, "CANCELLED")
    .when(col("Diverted") == 1, "DIVERTED")
    .otherwise("COMPLETED")
)

print("\n=== TRANSFORMATION RESULT ===")
print(f"Rows: {clean_df.count():,}")
print(f"Columns: {len(clean_df.columns)}")

print("\n=== FLIGHT STATUS ===")
clean_df.groupBy("flight_status").count().show()

print("\n=== SAMPLE ===")
clean_df.select(
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "flight_status"
).show(10, truncate=False)

spark.stop()