import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors


INPUT_FILE = "us_average_prices_monthly.csv"
OUTPUT_FILE = "validated_prices.csv"


ALLOWED_CATEGORIES = [
    "Beef",
    "Energy",
    "Pork and deli",
    "Vegetables",
    "Dairy and fats",
    "Fruit",
    "Bakery and grains",
    "Poultry",
    "Drinks",
    "Meat, broad category",
    "Pantry and snacks",
    "Eggs",
]


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
            Check.isin(ALLOWED_CATEGORIES),
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

    checks=[
        Check(
            lambda df:
                pd.to_datetime(df["date"]).dt.year
                == df["year"],
            error="date year must match year column",
        ),

        Check(
            lambda df:
                pd.to_datetime(df["date"]).dt.month
                == df["month"],
            error="date month must match month column",
        ),
    ],

    unique=["series_id", "date"],
    strict=True,
    coerce=True,
)


def transform_data(df):
    print("\nTransforming data...")

    df = df.copy()

    # Convert date from string to datetime temporarily
    df["date"] = pd.to_datetime(df["date"])

    # Add useful time features
    df["quarter"] = df["date"].dt.quarter

    df["year_month"] = (
        df["date"]
        .dt.to_period("M")
        .astype(str)
    )

    # Sort data consistently
    df = df.sort_values(
        ["series_id", "date"]
    )

    # Convert date back to YYYY-MM-DD for CSV output
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    return df


def run_pipeline():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 5: TRANSFORM")
    print("=" * 60)

    print(f"\nLoading: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    print(f"Rows loaded: {len(df):,}")

    try:
        print("\nValidating input data...")

        validated_df = schema.validate(
            df,
            lazy=True,
        )

        print("Input validation passed.")

    except SchemaErrors as exc:
        print("\nVALIDATION FAILED")
        print(exc.failure_cases)
        return

    transformed_df = transform_data(validated_df)

    print("\nNew columns added:")
    print("  ✓ quarter")
    print("  ✓ year_month")

    print("\nOUTPUT PREVIEW:")
    print(transformed_df.head())

    transformed_df.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print(f"\nValidated dataset written to:")
    print(OUTPUT_FILE)

    print(f"\nRows written: {len(transformed_df):,}")
    print(
        f"Columns written: "
        f"{len(transformed_df.columns)}"
    )

    print("\nSTEP 5 COMPLETE")


if __name__ == "__main__":
    run_pipeline()