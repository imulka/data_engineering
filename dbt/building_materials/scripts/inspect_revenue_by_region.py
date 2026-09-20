import duckdb

DB_PATH = "dev.duckdb"
TABLE_NAME = "fct_revenue_by_region"


def main():
    con = duckdb.connect(DB_PATH)

    print("\n" + "=" * 90)
    print("REVENUE BY REGION")
    print("=" * 90)

    cursor = con.execute(
        f"""
        SELECT
            region,
            transaction_count,
            total_units,
            total_revenue,
            avg_unit_price,
            avg_mortgage_rate
        FROM {TABLE_NAME}
        ORDER BY total_revenue DESC
        """
    )

    rows = cursor.fetchall()

    print(
        f"{'Region':25}"
        f"{'Transactions':15}"
        f"{'Units':15}"
        f"{'Revenue':18}"
        f"{'Avg Price':12}"
        f"{'Mortgage':10}"
    )

    print("-" * 95)

    for row in rows:
        print(
            f"{row[0]:25}"
            f"{row[1]:15,}"
            f"{row[2]:15,}"
            f"${row[3]:17,.2f}"
            f"${row[4]:11,.2f}"
            f"{row[5]:9.2f}%"
        )

    con.close()


if __name__ == "__main__":
    main()