# Data Engineering Practice Repository

This repository contains hands-on data engineering projects built to strengthen practical skills across modern data engineering tools and workflows.

The focus is on building small, complete pipelines that demonstrate how data is ingested, validated, transformed, stored, orchestrated, and analyzed.

## Technologies Practiced

* Python
* Pandas
* Apache Spark / PySpark
* Apache Airflow
* Apache Kafka
* dbt
* Pandera
* PostgreSQL
* SQLite
* Parquet
* Scikit-learn
* Git / GitHub

---

## Repository Structure

```text
data_engineering/
├── airflow/
├── dbt/
├── kafka/
├── pandera/
├── spark/
└── ...
```

Each directory contains an independent exercise or pipeline focused on a specific data engineering technology.

---

# Projects

## Apache Spark

Built an ETL pipeline using PySpark.

The pipeline includes:

* CSV ingestion
* Schema inspection
* Data profiling
* Data transformation
* Aggregations
* Parquet output
* PostgreSQL JDBC loading
* Machine learning with Spark ML

Example machine-learning results:

```text
AUC:       0.9708
Accuracy:  0.9035
Precision: 0.9036
Recall:    0.9035
```

---

## Apache Airflow

Built an Airflow DAG to practice workflow orchestration.

The exercise demonstrates:

* DAG creation
* Python tasks
* Task dependencies
* Data processing stages
* Data-quality checks
* Airflow local execution
* DAG testing and monitoring

Airflow is used to coordinate when individual data-engineering tasks should execute and in what order.

---

## Apache Kafka

Built a streaming pipeline using Apache Kafka.

The exercise demonstrates

**General Pipeline Philosophy**

Most exercises in this repository follow a similar pattern:

Extract
   ↓
Validate
   ↓
Transform
   ↓
Load / Store
   ↓
Quality Check
   ↓
Analyze

Some projects add orchestration, streaming, data modeling, or machine learning depending on the technology being practiced.
