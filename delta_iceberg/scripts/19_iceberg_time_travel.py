from pyspark.sql import SparkSession


OLD_SNAPSHOT = 2906684518609543475


spark = (
    SparkSession.builder
    .appName("Apache Iceberg - Time Travel")
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


print("\n=== OLD ICEBERG SNAPSHOT ===")

spark.sql(f"""
    SELECT flight_status, COUNT(*) AS count
    FROM local.bts.flights
    VERSION AS OF {OLD_SNAPSHOT}
    GROUP BY flight_status
""").show()


print("\n=== CURRENT ICEBERG SNAPSHOT ===")

spark.sql("""
    SELECT flight_status, COUNT(*) AS count
    FROM local.bts.flights
    GROUP BY flight_status
""").show()


spark.stop()