from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("NetflixChurnML")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.7"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# --------------------------------------------------
# PostgreSQL connection
# --------------------------------------------------

jdbc_url = "jdbc:postgresql://localhost:5432/netflix_etl"

connection_properties = {
    "user": "netflix_etl_user",
    "password": "netflix_etl_pw",
    "driver": "org.postgresql.Driver"
}


# --------------------------------------------------
# EXTRACT CLEAN DATA FROM POSTGRESQL
# --------------------------------------------------

df = spark.read.jdbc(
    url=jdbc_url,
    table="netflix_customers_clean",
    properties=connection_properties
)

print("\n=== DATA LOADED FROM POSTGRESQL ===")
print("Rows:", df.count())


# --------------------------------------------------
# FEATURES
# --------------------------------------------------

categorical_columns = [
    "gender",
    "subscription_type",
    "region",
    "device",
    "payment_method",
    "favorite_genre"
]

numeric_columns = [
    "age",
    "watch_hours",
    "last_login_days",
    "monthly_fee",
    "number_of_profiles",
    "avg_watch_time_per_day",
    "inactive_customer",
    "high_watch_user",
    "multiple_profiles"
]


# --------------------------------------------------
# STRING INDEXING
# --------------------------------------------------

indexers = [
    StringIndexer(
        inputCol=column,
        outputCol=f"{column}_index",
        handleInvalid="keep"
    )
    for column in categorical_columns
]


# --------------------------------------------------
# ONE-HOT ENCODING
# --------------------------------------------------

encoder = OneHotEncoder(
    inputCols=[
        f"{column}_index"
        for column in categorical_columns
    ],
    outputCols=[
        f"{column}_encoded"
        for column in categorical_columns
    ]
)


# --------------------------------------------------
# VECTOR ASSEMBLER
# --------------------------------------------------

assembler = VectorAssembler(
    inputCols=
        numeric_columns +
        [
            f"{column}_encoded"
            for column in categorical_columns
        ],
    outputCol="features"
)


# --------------------------------------------------
# LOGISTIC REGRESSION
# --------------------------------------------------

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churned",
    predictionCol="prediction",
    probabilityCol="probability",
    maxIter=100
)


# --------------------------------------------------
# BUILD ML PIPELINE
# --------------------------------------------------

pipeline = Pipeline(
    stages=indexers + [
        encoder,
        assembler,
        lr
    ]
)


# --------------------------------------------------
# TRAIN / TEST SPLIT
# --------------------------------------------------

train_df, test_df = df.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("\n=== TRAIN / TEST ===")
print("Training rows:", train_df.count())
print("Testing rows:", test_df.count())


# --------------------------------------------------
# TRAIN MODEL
# --------------------------------------------------

print("\n=== TRAINING MODEL ===")

model = pipeline.fit(train_df)

print("Training complete.")


# --------------------------------------------------
# PREDICTIONS
# --------------------------------------------------

predictions = model.transform(test_df)

print("\n=== SAMPLE PREDICTIONS ===")

predictions.select(
    "customer_id",
    "subscription_type",
    "last_login_days",
    "avg_watch_time_per_day",
    "churned",
    "prediction",
    "probability"
).show(15, truncate=False)


# --------------------------------------------------
# AUC
# --------------------------------------------------

auc_evaluator = BinaryClassificationEvaluator(
    labelCol="churned",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = auc_evaluator.evaluate(predictions)


# --------------------------------------------------
# ACCURACY
# --------------------------------------------------

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="churned",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = accuracy_evaluator.evaluate(predictions)


# --------------------------------------------------
# PRECISION
# --------------------------------------------------

precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="churned",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

precision = precision_evaluator.evaluate(predictions)


# --------------------------------------------------
# RECALL
# --------------------------------------------------

recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="churned",
    predictionCol="prediction",
    metricName="weightedRecall"
)

recall = recall_evaluator.evaluate(predictions)


print("\n=== MODEL RESULTS ===")

print(f"AUC:       {auc:.4f}")
print(f"Accuracy:  {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")


# --------------------------------------------------
# SAVE PREDICTIONS TO POSTGRESQL
# --------------------------------------------------

prediction_output = predictions.select(
    "customer_id",
    "churned",
    "prediction"
)

prediction_output.write.jdbc(
    url=jdbc_url,
    table="churn_predictions",
    mode="overwrite",
    properties=connection_properties
)

print("\nPredictions saved to PostgreSQL table: churn_predictions")

print("\n=== CONFUSION MATRIX ===")

predictions.groupBy(
    "churned",
    "prediction"
).count().orderBy(
    "churned",
    "prediction"
).show()


# --------------------------------------------------
# SAVE ML REPORT TO FILE
# --------------------------------------------------

report_path = "output/ml_report.txt"

sample_predictions = predictions.select(
    "customer_id",
    "subscription_type",
    "last_login_days",
    "avg_watch_time_per_day",
    "churned",
    "prediction",
    "probability"
).limit(15).collect()

confusion_rows = (
    predictions.groupBy("churned", "prediction")
    .count()
    .orderBy("churned", "prediction")
    .collect()
)

with open(report_path, "w") as f:
    f.write("NETFLIX CHURN ML REPORT\n")
    f.write("=======================\n\n")

    f.write("MODEL USED\n")
    f.write("Logistic Regression\n\n")

    f.write("TRAIN / TEST\n")
    f.write(f"Training rows: {train_df.count()}\n")
    f.write(f"Testing rows: {test_df.count()}\n\n")

    f.write("MODEL RESULTS\n")
    f.write(f"AUC:       {auc:.4f}\n")
    f.write(f"Accuracy:  {accuracy:.4f}\n")
    f.write(f"Precision: {precision:.4f}\n")
    f.write(f"Recall:    {recall:.4f}\n\n")

    f.write("SAMPLE PREDICTIONS\n")
    f.write("------------------\n")

    for row in sample_predictions:
        f.write(
            f"customer_id={row.customer_id}, "
            f"subscription={row.subscription_type}, "
            f"last_login_days={row.last_login_days}, "
            f"avg_watch_time_per_day={row.avg_watch_time_per_day}, "
            f"actual={row.churned}, "
            f"prediction={row.prediction}, "
            f"probability={row.probability}\n"
        )

    f.write("\nCONFUSION MATRIX\n")
    f.write("----------------\n")

    for row in confusion_rows:
        f.write(
            f"actual={row.churned}, "
            f"prediction={row.prediction}, "
            f"count={row['count']}\n"
        )

print(f"\nML report saved to: {report_path}")

spark.stop()