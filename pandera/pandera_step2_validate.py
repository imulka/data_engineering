import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors


INPUT_FILE = "us_average_prices_monthly.csv"


schema = pa.DataFrameSchema(
    {
        "date": pa.Column(str, nullable=False),

        "year": pa.Column(
            int,
            Check.in_range(2015, 2026),
            nullable=False,
        ),

        "month": pa.Column(
            int,
            Check.in_range(1, 12),
            nullable=False,
        ),

        "item": pa.Column(
            str,
            Check.str_length(min_value=1),
            nullable=False,
        ),

        "unit": pa.Column(
            str,
            Check.str_length(min_value=1),
            nullable=False,
        ),

        "category": pa.Column(
            str,
            Check.str_length(min_value=1),
            nullable=False,
        ),

        "price": pa.Column(
            float,
            Check.gt(0),
            nullable=False,
        ),

        "series_id": pa.Column(
            str,
            Check.str_length(min_value=1),
            nullable=False,
        ),
    },
    strict=True,
    coerce=True,
)


def validate_data():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 2: SCHEMA VALIDATION")
    print("=" * 60)

    print(f"\nLoading: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    print(f"Rows loaded: {len(df):,}")

    try:
        validated_df = schema.validate(
            df,
            lazy=True,
        )

        print("\nVALIDATION PASSED")
        print(f"Validated rows: {len(validated_df):,}")
        print(f"Validated columns: {len(validated_df.columns)}")

        print("\nDATA TYPES AFTER VALIDATION:")
        print(validated_df.dtypes)

        print("\nAll schema rules passed.")

    except SchemaErrors as exc:
        print("\nVALIDATION FAILED")

        print("\nFAILURE CASES:")
        print(exc.failure_cases)

        print(f"\nTotal failures: {len(exc.failure_cases)}")


if __name__ == "__main__":
    validate_data()