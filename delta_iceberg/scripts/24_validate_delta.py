from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("Delta - Final Validation")
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


df = (
    spark.read
    .format("delta")
    .load("warehouse/delta/flights")
)


print("\n=== FINAL DELTA VALIDATION ===")

print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

df.groupBy("flight_status").count().show()

print("Missing routes:", df.filter(df.route.isNull()).count())


spark.stop()