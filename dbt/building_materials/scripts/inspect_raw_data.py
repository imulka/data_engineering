import duckdb

DB_PATH = "dev.duckdb"
TABLE_NAME = "building_materials_transactions"


def main():
    con = duckdb.connect(DB_PATH)

    print("\n" + "=" * 70)
    print("TABLE SCHEMA")
    print("=" * 70)

    schema = con.execute(
        f"DESCRIBE {TABLE_NAME}"
    ).fetchall()

    for column in schema:
        print(
            f"{column[0]:25} "
            f"{column[1]:15} "
            f"NULLABLE={column[2]}"
        )

    print("\n" + "=" * 70)
    print("ROW COUNT")
    print("=" * 70)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {TABLE_NAME}"
    ).fetchone()[0]

    print(f"Total rows: {row_count:,}")

    print("\n" + "=" * 70)
    print("SAMPLE DATA")
    print("=" * 70)

    cursor = con.execute(
        f"""
        SELECT *
        FROM {TABLE_NAME}
        LIMIT 5
        """
    )

    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    print(" | ".join(column_names))
    print("-" * 180)

    for row in rows:
        print(" | ".join(str(value) for value in row))

    con.close()


if __name__ == "__main__":
    main()