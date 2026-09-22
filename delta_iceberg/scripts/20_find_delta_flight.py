from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


DELTA_PATH = "warehouse/delta/flights"


builder = (
    SparkSession.builder
    .appName("Delta Lake - Find Flight")
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
    .load(DELTA_PATH)
)


df.filter(
    (df.FlightDate == "2026-07-28") &
    (df.Reporting_Airline == "DL") &
    (df.Flight_Number_Reporting_Airline == 1263) &
    (df.Origin == "ATL") &
    (df.Dest == "RIC")
).select(
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "flight_status"
).show(truncate=False)


spark.stop()