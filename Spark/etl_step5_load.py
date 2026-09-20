from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("NetflixChurnLoad")
    .master("local[*]")
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

print("\n=== WRITING CLEAN DATA ===")

df_clean.write \
    .mode("overwrite") \
    .parquet("output/netflix_churn_clean")

print("Write complete.")

print("\n=== READING PARQUET BACK ===")

df_loaded = spark.read.parquet(
    "output/netflix_churn_clean"
)

print("Rows loaded:", df_loaded.count())

df_loaded.printSchema()

df_loaded.show(5, truncate=False)

spark.stop()