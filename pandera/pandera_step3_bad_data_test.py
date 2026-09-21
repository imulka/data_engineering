import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors


INPUT_FILE = "us_average_prices_monthly.csv"


schema = pa.DataFrameSchema(
    {
        "date": pa.Column(
            str,
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


def create_bad_data(df):
    """
    Deliberately corrupt several rows so that
    Pandera can demonstrate data-quality failures.
    """

    bad_df = df.copy()

    # Invalid month
    bad_df.loc[0, "month"] = 13

    # Invalid year
    bad_df.loc[1, "year"] = 2035

    # Invalid negative price
    bad_df.loc[2, "price"] = -5.00

    # Invalid zero price
    bad_df.loc[3, "price"] = 0.00

    # Empty item
    bad_df.loc[4, "item"] = ""

    return bad_df


def test_validation():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 3: BAD DATA TEST")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nOriginal rows: {len(df):,}")

    bad_df = create_bad_data(df)

    print("\nInjected bad records:")
    print(
        bad_df.loc[
            0:4,
            [
                "date",
                "year",
                "month",
                "item",
                "price",
            ],
        ]
    )

    print("\nRunning Pandera validation...")

    try:
        schema.validate(
            bad_df,
            lazy=True,
        )

        print("\nWARNING: Validation unexpectedly passed.")

    except SchemaErrors as exc:

        print("\nVALIDATION FAILED AS EXPECTED")

        print("\nFAILURE CASES:")
        print(
            exc.failure_cases[
                [
                    "schema_context",
                    "column",
                    "check",
                    "failure_case",
                    "index",
                ]
            ].to_string(index=False)
        )

        print(
            f"\nPandera detected "
            f"{len(exc.failure_cases)} validation failures."
        )


if __name__ == "__main__":
    test_validation()