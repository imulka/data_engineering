from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BTS Flight Data - Flight Status Profiling")
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

print("\n=== CANCELLED FLIGHTS ===")
df.groupBy("Cancelled").count().orderBy("Cancelled").show()

print("\n=== DIVERTED FLIGHTS ===")
df.groupBy("Diverted").count().orderBy("Diverted").show()

print("\n=== CANCELLATION CODES ===")
df.groupBy("CancellationCode").count().orderBy("CancellationCode").show()

spark.stop()