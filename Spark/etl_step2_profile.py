from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("NetflixChurnProfile")
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

print("\n=== ROW COUNT ===")
print(df.count())

print("\n=== DUPLICATE ROWS ===")
duplicate_count = df.count() - df.dropDuplicates().count()
print(duplicate_count)

print("\n=== NULL COUNTS ===")
df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show(truncate=False)

print("\n=== CHURN DISTRIBUTION ===")
df.groupBy("churned").count().orderBy("churned").show()

print("\n=== SUBSCRIPTION TYPES ===")
df.groupBy("subscription_type").count().orderBy("subscription_type").show()

print("\n=== REGIONS ===")
df.groupBy("region").count().orderBy("region").show()

print("\n=== DEVICES ===")
df.groupBy("device").count().orderBy("device").show()

print("\n=== PAYMENT METHODS ===")
df.groupBy("payment_method").count().orderBy("payment_method").show()

print("\n=== NUMERIC SUMMARY ===")
df.select(
    "age",
    "watch_hours",
    "last_login_days",
    "monthly_fee",
    "number_of_profiles",
    "avg_watch_time_per_day"
).describe().show(truncate=False)

spark.stop()