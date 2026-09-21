import numpy as np
import pandas as pd


INPUT_FILE = "validated_prices.csv"
OUTPUT_FILE = "ml_price_features.csv"


def build_features(df):
    df = df.copy()

    # Convert date back to datetime for feature engineering
    df["date"] = pd.to_datetime(df["date"])

    # Continuous month counter representing time progression
    absolute_month = (
        df["date"].dt.year * 12
        + df["date"].dt.month
    )

    df["time_index"] = (
        absolute_month - absolute_month.min()
    )

    # Cyclical month features
    # December and January should be "close" mathematically.
    df["month_sin"] = np.sin(
        2 * np.pi * df["month"] / 12
    )

    df["month_cos"] = np.cos(
        2 * np.pi * df["month"] / 12
    )

    feature_columns = [
        "date",
        "year",
        "month",
        "quarter",
        "time_index",
        "month_sin",
        "month_cos",
        "item",
        "unit",
        "category",
        "price",
    ]

    return df[feature_columns]


def run_feature_pipeline():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 6: ML FEATURE ENGINEERING")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nRows loaded: {len(df):,}")

    ml_df = build_features(df)

    print("\nFEATURES CREATED:")
    print("  ✓ time_index")
    print("  ✓ month_sin")
    print("  ✓ month_cos")

    print("\nCATEGORICAL FEATURE CARDINALITY:")
    print(f"  Items:      {ml_df['item'].nunique():,}")
    print(f"  Units:      {ml_df['unit'].nunique():,}")
    print(f"  Categories: {ml_df['category'].nunique():,}")

    print("\nNUMERIC FEATURES:")
    print("  year")
    print("  month")
    print("  quarter")
    print("  time_index")
    print("  month_sin")
    print("  month_cos")

    print("\nTARGET:")
    print("  price")

    print("\nFEATURE PREVIEW:")
    print(ml_df.head())

    # Store date consistently in the CSV
    ml_df["date"] = ml_df["date"].dt.strftime("%Y-%m-%d")

    ml_df.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print(f"\nML dataset written to: {OUTPUT_FILE}")
    print(f"Rows written: {len(ml_df):,}")
    print(f"Columns written: {len(ml_df.columns)}")

    print("\nSTEP 6 COMPLETE")


if __name__ == "__main__":
    run_feature_pipeline()