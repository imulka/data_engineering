from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("Delta Lake - Time Travel")
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


print("\n=== VERSION 0: BEFORE UPDATE ===")

version_0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(DELTA_PATH)
)

version_0.groupBy("flight_status").count().show()


print("\n=== CURRENT VERSION: AFTER UPDATE ===")

current = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)

current.groupBy("flight_status").count().show()


spark.stop()