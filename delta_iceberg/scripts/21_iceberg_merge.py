from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Apache Iceberg - Merge")
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


print("\n=== ICEBERG MERGE ===")

spark.sql("""
    MERGE INTO local.bts.flights AS target

    USING (
        SELECT
            DATE '2026-07-28' AS FlightDate,
            'DL' AS Reporting_Airline,
            1263 AS Flight_Number_Reporting_Airline,
            'ATL' AS Origin,
            'RIC' AS Dest,
            2012 AS CRSDepTime,
            'CORRECTED' AS flight_status
    ) AS source

    ON target.FlightDate = source.FlightDate
    AND target.Reporting_Airline = source.Reporting_Airline
    AND target.Flight_Number_Reporting_Airline =
        source.Flight_Number_Reporting_Airline
    AND target.Origin = source.Origin
    AND target.Dest = source.Dest
    AND target.CRSDepTime = source.CRSDepTime

    WHEN MATCHED THEN
        UPDATE SET target.flight_status = source.flight_status
""")


print("\n=== VERIFY RECORD ===")

spark.sql("""
    SELECT
        FlightDate,
        Reporting_Airline,
        Flight_Number_Reporting_Airline,
        Origin,
        Dest,
        CRSDepTime,
        flight_status
    FROM local.bts.flights
    WHERE FlightDate = DATE '2026-07-28'
      AND Reporting_Airline = 'DL'
      AND Flight_Number_Reporting_Airline = 1263
      AND Origin = 'ATL'
      AND Dest = 'RIC'
      AND CRSDepTime = 2012
""").show(truncate=False)


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