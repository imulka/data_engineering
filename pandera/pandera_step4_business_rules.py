import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors


INPUT_FILE = "us_average_prices_monthly.csv"


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
        "date": pa.Column(
            str,
            Check(
                lambda s: pd.to_datetime(
                    s,
                    errors="coerce"
                ).notna(),
                error="date must be a valid date",
            ),
            nullable=False,
        ),

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


def validate_business_rules():

    print("=" * 60)
    print("PANDERA PIPELINE - STEP 4: BUSINESS RULES")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nRows loaded: {len(df):,}")

    print("\nRunning business-rule validation...")

    try:

        validated_df = schema.validate(
            df,
            lazy=True,
        )

        print("\nBUSINESS RULE VALIDATION PASSED")

        print(f"Validated rows: {len(validated_df):,}")

        print("\nRules verified:")
        print("  ✓ Valid dates")
        print("  ✓ Date year matches year column")
        print("  ✓ Date month matches month column")
        print("  ✓ Valid categories")
        print("  ✓ Positive prices")
        print("  ✓ Unique series_id + date")
        print("  ✓ No unexpected columns")

    except SchemaErrors as exc:

        print("\nBUSINESS RULE VALIDATION FAILED")

        print("\nFAILURE CASES:")

        print(
            exc.failure_cases.to_string(
                index=False
            )
        )

        print(
            f"\nTotal failures: "
            f"{len(exc.failure_cases)}"
        )


if __name__ == "__main__":
    validate_business_rules()