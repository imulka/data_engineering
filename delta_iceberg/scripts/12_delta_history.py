from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("Delta Lake - History")
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

print("\n=== DELTA TABLE HISTORY ===")

delta_table.history().select(
    "version",
    "timestamp",
    "operation",
    "operationParameters"
).show(truncate=False)


spark.stop()