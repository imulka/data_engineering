from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import Row


DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("Delta Lake - Merge")
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


# Create one incoming correction for an existing flight
source_df = spark.createDataFrame([
    Row(
        FlightDate="2026-07-28",
        Reporting_Airline="DL",
        Flight_Number_Reporting_Airline=1263,
        Origin="ATL",
        Dest="RIC",
        CRSDepTime=2012,
        flight_status="CORRECTED"
    )
])


print("\n=== MERGING RECORD ===")

(
    delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        """
        target.FlightDate = source.FlightDate
        AND target.Reporting_Airline = source.Reporting_Airline
        AND target.Flight_Number_Reporting_Airline =
            source.Flight_Number_Reporting_Airline
        AND target.Origin = source.Origin
        AND target.Dest = source.Dest
        AND target.CRSDepTime = source.CRSDepTime
        """
    )
    .whenMatchedUpdate(
        set={
            "flight_status": "source.flight_status"
        }
    )
    .execute()
)


print("\n=== DELTA HISTORY ===")

delta_table.history().select(
    "version",
    "timestamp",
    "operation"
).show(truncate=False)


spark.stop()