from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import sqlite3

CSV_PATH = "/home/isaacm/etlworks/Airflow/project1/online_food_delivery_dataset.csv"
CLEAN_PATH = "/home/isaacm/etlworks/Airflow/project1/clean_food_delivery.csv"
DB_PATH = "/home/isaacm/etlworks/Airflow/project1/food_delivery.db"

@dag(
    dag_id="food_delivery_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "food-delivery"],
)
def food_delivery_etl():

    @task
    def extract():
        df = pd.read_csv(CSV_PATH)
        print(f"Rows extracted: {len(df)}")
        return CSV_PATH

    @task
    def transform(csv_path):
        df = pd.read_csv(csv_path)

        df = df.drop(columns=["Unnamed: 13"], errors="ignore")

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        df["feedback"] = df["feedback"].str.strip()

        df.to_csv(CLEAN_PATH, index=False)

        print(f"Rows transformed: {len(df)}")

        return CLEAN_PATH

    @task
    def load(clean_path):
        df = pd.read_csv(clean_path)

        # Stable row identifier for this source file
        df.insert(0, "source_row_id", range(1, len(df) + 1))

        conn = sqlite3.connect(DB_PATH)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS food_orders (
                source_row_id INTEGER PRIMARY KEY,
                age INTEGER,
                gender TEXT,
                marital_status TEXT,
                occupation TEXT,
                monthly_income TEXT,
                educational_qualifications TEXT,
                family_size INTEGER,
                customer_type TEXT,
                latitude REAL,
                longitude REAL,
                pin_code INTEGER,
                output TEXT,
                feedback TEXT
            )
        """)

        before = conn.execute(
            "SELECT COUNT(*) FROM food_orders"
        ).fetchone()[0]

        df.to_sql(
            "staging_food_orders",
            conn,
            if_exists="replace",
            index=False
        )

        conn.execute("""
            INSERT OR IGNORE INTO food_orders
            SELECT * FROM staging_food_orders
        """)

        conn.commit()

        after = conn.execute(
            "SELECT COUNT(*) FROM food_orders"
        ).fetchone()[0]

        conn.close()

        print(f"Rows before: {before}")
        print(f"Rows after: {after}")
        print(f"New rows inserted: {after - before}")

    @task
    def data_quality():
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM food_orders")
        row_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM food_orders
            WHERE age IS NULL
               OR gender IS NULL
               OR feedback IS NULL
        """)
        null_count = cur.fetchone()[0]

        conn.close()

        if row_count == 0:
            raise ValueError("Data quality failed: table is empty")

        if null_count > 0:
            raise ValueError(
                f"Data quality failed: {null_count} rows contain NULL key values"
            )

        print(f"Data quality passed")
        print(f"Rows checked: {row_count}")
        print(f"NULL key rows: {null_count}")

    
    @task
    def aggregate():
        conn = sqlite3.connect(DB_PATH)

        query = """
        SELECT
            occupation,
            customer_type,
            feedback,
            COUNT(*) AS customer_count
        FROM food_orders
        GROUP BY occupation, customer_type, feedback
        ORDER BY customer_count DESC
        """

        df = pd.read_sql_query(query, conn)

        df.to_sql(
            "food_order_summary",
            conn,
            if_exists="replace",
            index=False
        )

        conn.close()

        print(df.head(10))
        print(f"Summary rows created: {len(df)}")

    raw_file = extract()
    clean_file = transform(raw_file)
    loaded = load(clean_file)

    quality = data_quality()
    summary = aggregate()

    loaded >> quality >> summary

food_delivery_etl()
