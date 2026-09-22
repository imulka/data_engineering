from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Apache Iceberg - Update")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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
        "warehouse/iceberg"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n=== UPDATING DELAYED FLIGHTS ===")

spark.sql("""
    UPDATE local.bts.flights
    SET flight_status = 'DELAYED'
    WHERE Cancelled = 0
      AND Diverted = 0
      AND ArrDel15 = 1
""")


print("\n=== UPDATED FLIGHT STATUS COUNTS ===")

spark.sql("""
    SELECT flight_status, COUNT(*) AS count
    FROM local.bts.flights
    GROUP BY flight_status
    ORDER BY flight_status
""").show()


print("\n=== ICEBERG SNAPSHOTS ===")

spark.sql("""
    SELECT
        committed_at,
        snapshot_id,
        operation
    FROM local.bts.flights.snapshots
    ORDER BY committed_at DESC
""").show(truncate=False)


spark.stop()