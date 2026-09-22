from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Iceberg - Final Validation")
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


df = spark.table("local.bts.flights")


print("\n=== FINAL ICEBERG VALIDATION ===")

print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

df.groupBy("flight_status").count().show()

print("Missing routes:", df.filter(df.route.isNull()).count())


spark.stop()