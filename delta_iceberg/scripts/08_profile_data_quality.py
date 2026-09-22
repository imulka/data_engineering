from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Quality Checks")
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

print("\n=== CORE DATA QUALITY CHECKS ===")

print(
    "Distance <= 0:",
    df.filter(col("Distance") <= 0).count()
)

print(
    "CRSElapsedTime <= 0:",
    df.filter(col("CRSElapsedTime") <= 0).count()
)

print(
    "Origin = Destination:",
    df.filter(col("Origin") == col("Dest")).count()
)

print(
    "Missing Origin:",
    df.filter(col("Origin").isNull()).count()
)

print(
    "Missing Destination:",
    df.filter(col("Dest").isNull()).count()
)

print(
    "Invalid Cancelled flag:",
    df.filter(~col("Cancelled").isin(0.0, 1.0)).count()
)

print(
    "Invalid Diverted flag:",
    df.filter(~col("Diverted").isin(0.0, 1.0)).count()
)

spark.stop()