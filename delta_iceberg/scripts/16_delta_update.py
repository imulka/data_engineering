from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("Delta Lake - Update")
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


delta_table = DeltaTable.forPath(spark, DELTA_PATH)


print("\n=== UPDATING DELAYED FLIGHTS ===")

delta_table.update(
    condition="""
        Cancelled = 0
        AND Diverted = 0
        AND ArrDel15 = 1
    """,
    set={
        "flight_status": "'DELAYED'"
    }
)


print("\n=== UPDATED FLIGHT STATUS COUNTS ===")

spark.read \
    .format("delta") \
    .load(DELTA_PATH) \
    .groupBy("flight_status") \
    .count() \
    .show()


print("\n=== DELTA HISTORY ===")

delta_table.history().select(
    "version",
    "timestamp",
    "operation"
).show(truncate=False)


spark.stop()