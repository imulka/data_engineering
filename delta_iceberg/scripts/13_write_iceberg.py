from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


INPUT_FILE = (
    "data/raw/"
    "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2026_7.csv"
)

ICEBERG_WAREHOUSE = "warehouse/iceberg"


spark = (
    SparkSession.builder
    .appName("BTS Flights - Apache Iceberg")
    .master("local[*]")

    # Load Iceberg runtime
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
    )

    # Enable Iceberg SQL features
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Create a local Iceberg catalog
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        ICEBERG_WAREHOUSE
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read raw CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_FILE)
)


# Apply same transformations used for Delta
clean_df = (
    df
    .drop("_c109")
    .withColumn(
        "Cancelled",
        col("Cancelled").cast("integer")
    )
    .withColumn(
        "Diverted",
        col("Diverted").cast("integer")
    )
    .withColumn(
        "flight_status",
        when(col("Cancelled") == 1, "CANCELLED")
        .when(col("Diverted") == 1, "DIVERTED")
        .otherwise("COMPLETED")
    )
)


print("\n=== WRITING ICEBERG TABLE ===")

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS local.bts"
)

clean_df.writeTo(
    "local.bts.flights"
).using(
    "iceberg"
).createOrReplace()


print("\n=== READING ICEBERG TABLE BACK ===")

iceberg_df = spark.table(
    "local.bts.flights"
)

print(f"Iceberg rows: {iceberg_df.count():,}")
print(f"Iceberg columns: {len(iceberg_df.columns)}")


spark.stop()