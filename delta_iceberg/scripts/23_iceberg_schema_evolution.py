from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Apache Iceberg - Schema Evolution")
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


print("\n=== ADDING ROUTE COLUMN ===")

spark.sql("""
    ALTER TABLE local.bts.flights
    ADD COLUMN route STRING
""")


print("\n=== POPULATING ROUTE ===")

spark.sql("""
    UPDATE local.bts.flights
    SET route = CONCAT(Origin, '-', Dest)
""")


print("\n=== SAMPLE ===")

spark.sql("""
    SELECT Origin, Dest, route
    FROM local.bts.flights
    LIMIT 10
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