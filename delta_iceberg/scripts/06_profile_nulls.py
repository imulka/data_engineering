from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Null Profiling")
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

total_rows = df.count()

null_counts = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).collect()[0]

print(f"\nTotal rows: {total_rows:,}\n")

for column in df.columns:
    nulls = null_counts[column]
    percent = (nulls / total_rows) * 100

    if nulls > 0:
        print(
            f"{column:30} "
            f"{nulls:>10,} NULLs "
            f"({percent:6.2f}%)"
        )

spark.stop()