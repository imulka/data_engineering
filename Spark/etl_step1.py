from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NetflixChurnETL")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("netflix_customer_churn.csv")
)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== FIRST 10 ROWS ===")
df.show(10, truncate=False)

print("\n=== ROW COUNT ===")
print(df.count())

print("\n=== COLUMNS ===")
print(df.columns)

spark.stop()