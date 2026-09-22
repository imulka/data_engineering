from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from delta import configure_spark_with_delta_pip


INPUT_FILE = (
    "data/raw/"
    "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2026_7.csv"
)

DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("BTS Flights - Delta Lake")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Read raw CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_FILE)
)


# Apply our cleaning/transformation rules
clean_df = (
    df
    .drop("_c109")
    .withColumn("Cancelled", col("Cancelled").cast("integer"))
    .withColumn("Diverted", col("Diverted").cast("integer"))
    .withColumn(
        "flight_status",
        when(col("Cancelled") == 1, "CANCELLED")
        .when(col("Diverted") == 1, "DIVERTED")
        .otherwise("COMPLETED")
    )
)


print("\n=== WRITING DELTA TABLE ===")

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(DELTA_PATH)


print("\n=== READING DELTA TABLE BACK ===")

delta_df = spark.read \
    .format("delta") \
    .load(DELTA_PATH)

print(f"Delta rows: {delta_df.count():,}")
print(f"Delta columns: {len(delta_df.columns)}")


spark.stop()