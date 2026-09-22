import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

DELTA_PATH = os.path.abspath("warehouse/delta/flights")


builder = (
    SparkSession.builder
    .appName("Delta Lake - Schema Evolution")
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


# Register Delta path as a temporary SQL table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS flights_delta
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")


print("\n=== ADDING ROUTE COLUMN ===")

spark.sql("""
    ALTER TABLE flights_delta
    ADD COLUMN route STRING
""")


print("\n=== POPULATING ROUTE ===")

spark.sql("""
    UPDATE flights_delta
    SET route = CONCAT(Origin, '-', Dest)
""")


print("\n=== SAMPLE ===")

spark.sql("""
    SELECT Origin, Dest, route
    FROM flights_delta
    LIMIT 10
""").show()


print("\n=== DELTA HISTORY ===")

DeltaTable.forPath(
    spark,
    DELTA_PATH
).history().select(
    "version",
    "operation"
).show()


spark.stop()