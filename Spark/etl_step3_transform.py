from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("NetflixChurnTransform")
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

print("\n=== ORIGINAL ROWS ===")
print(df.count())


# --------------------------------------------------
# 1. Remove duplicate rows
# --------------------------------------------------

df_clean = df.dropDuplicates()


# --------------------------------------------------
# 2. Remove impossible daily watch-time values
# --------------------------------------------------

df_clean = df_clean.filter(
    (col("avg_watch_time_per_day") >= 0)
    & (col("avg_watch_time_per_day") <= 24)
)


# --------------------------------------------------
# 3. Validate age
# --------------------------------------------------

df_clean = df_clean.filter(
    (col("age") >= 18)
    & (col("age") <= 100)
)


# --------------------------------------------------
# 4. Validate churn label
# --------------------------------------------------

df_clean = df_clean.filter(
    col("churned").isin(0, 1)
)


# --------------------------------------------------
# 5. Create useful derived columns
# --------------------------------------------------

df_clean = df_clean.withColumn(
    "inactive_customer",
    when(col("last_login_days") >= 30, 1).otherwise(0)
)

df_clean = df_clean.withColumn(
    "high_watch_user",
    when(col("avg_watch_time_per_day") >= 2, 1).otherwise(0)
)

df_clean = df_clean.withColumn(
    "multiple_profiles",
    when(col("number_of_profiles") > 1, 1).otherwise(0)
)


print("\n=== CLEAN ROW COUNT ===")
print(df_clean.count())


print("\n=== CLEAN SCHEMA ===")
df_clean.printSchema()


print("\n=== SAMPLE TRANSFORMED DATA ===")

df_clean.select(
    "customer_id",
    "age",
    "avg_watch_time_per_day",
    "last_login_days",
    "inactive_customer",
    "high_watch_user",
    "number_of_profiles",
    "multiple_profiles",
    "churned"
).show(10, truncate=False)


print("\n=== MAX WATCH TIME AFTER CLEANING ===")

df_clean.select(
    "avg_watch_time_per_day"
).describe().show()


spark.stop()