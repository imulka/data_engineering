from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("NetflixChurnPostgresLoad")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.7"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("netflix_customer_churn.csv")
)

df_clean = (
    df.dropDuplicates()
      .filter(
          (col("avg_watch_time_per_day") >= 0) &
          (col("avg_watch_time_per_day") <= 24)
      )
      .filter(
          (col("age") >= 18) &
          (col("age") <= 100)
      )
      .filter(col("churned").isin(0, 1))
      .withColumn(
          "inactive_customer",
          when(col("last_login_days") >= 30, 1).otherwise(0)
      )
      .withColumn(
          "high_watch_user",
          when(col("avg_watch_time_per_day") >= 2, 1).otherwise(0)
      )
      .withColumn(
          "multiple_profiles",
          when(col("number_of_profiles") > 1, 1).otherwise(0)
      )
)

jdbc_url = "jdbc:postgresql://localhost:5432/netflix_etl"

connection_properties = {
    "user": "netflix_etl_user",
    "password": "netflix_etl_pw",
    "driver": "org.postgresql.Driver"
}

print("\n=== WRITING TO POSTGRESQL ===")

df_clean.write.jdbc(
    url=jdbc_url,
    table="netflix_customers_clean",
    mode="overwrite",
    properties=connection_properties
)

print("PostgreSQL load complete.")

print("\n=== READING BACK FROM POSTGRESQL ===")

df_db = spark.read.jdbc(
    url=jdbc_url,
    table="netflix_customers_clean",
    properties=connection_properties
)

print("Rows read from PostgreSQL:", df_db.count())

df_db.select(
    "customer_id",
    "subscription_type",
    "last_login_days",
    "inactive_customer",
    "churned"
).show(10, truncate=False)

spark.stop()