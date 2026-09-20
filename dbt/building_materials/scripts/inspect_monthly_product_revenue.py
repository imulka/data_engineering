import duckdb

DB_PATH = "dev.duckdb"
TABLE_NAME = "fct_monthly_product_revenue"


def main():
    con = duckdb.connect(DB_PATH)

    print("\n" + "=" * 95)
    print("MONTHLY PRODUCT REVENUE")
    print("=" * 95)

    cursor = con.execute(
        f"""
        SELECT
            year,
            month,
            product_category,
            transaction_count,
            total_units,
            total_revenue,
            avg_unit_price
        FROM {TABLE_NAME}
        ORDER BY year, month, total_revenue DESC
        LIMIT 30
        """
    )

    rows = cursor.fetchall()

    print(
        f"{'Year':<7}"
        f"{'Month':<8}"
        f"{'Product Category':<25}"
        f"{'Transactions':>15}"
        f"{'Units':>15}"
        f"{'Revenue':>20}"
        f"{'Avg Price':>15}"
    )

    print("-" * 105)

    for row in rows:
        print(
            f"{row[0]:<7}"
            f"{row[1]:<8}"
            f"{row[2]:<25}"
            f"{row[3]:>15,}"
            f"{row[4]:>15,}"
            f"${row[5]:>19,.2f}"
            f"${row[6]:>14,.2f}"
        )

    con.close()


if __name__ == "__main__":
    main()