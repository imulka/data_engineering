from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, round

spark = (
    SparkSession.builder
    .appName("NetflixChurnAggregate")
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

# Clean
df = (
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
)

# Feature engineering
df = (
    df.withColumn(
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


print("\n=== OVERALL CHURN RATE ===")

df.select(
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).show()


print("\n=== CHURN BY SUBSCRIPTION ===")

df.groupBy("subscription_type").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy(col("churn_rate_pct").desc()).show()


print("\n=== CHURN BY REGION ===")

df.groupBy("region").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy(col("churn_rate_pct").desc()).show()


print("\n=== CHURN BY DEVICE ===")

df.groupBy("device").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy(col("churn_rate_pct").desc()).show()


print("\n=== CHURN BY INACTIVITY ===")

df.groupBy("inactive_customer").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy("inactive_customer").show()


print("\n=== CHURN BY WATCH BEHAVIOR ===")

df.groupBy("high_watch_user").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy("high_watch_user").show()


print("\n=== CHURN BY PROFILE COUNT BEHAVIOR ===")

df.groupBy("multiple_profiles").agg(
    count("*").alias("customers"),
    round(avg("churned") * 100, 2).alias("churn_rate_pct")
).orderBy("multiple_profiles").show()


spark.stop()