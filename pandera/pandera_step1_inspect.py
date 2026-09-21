import pandas as pd


INPUT_FILE = "us_average_prices_monthly.csv"


def inspect_data():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 1: DATA INSPECTION")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nInput file: {INPUT_FILE}")

    print("\nSHAPE:")
    print(df.shape)

    print("\nCOLUMNS:")
    print(df.columns.tolist())

    print("\nDATA TYPES:")
    print(df.dtypes)

    print("\nFIRST 10 ROWS:")
    print(df.head(10))

    print("\nNULL COUNTS:")
    print(df.isnull().sum())

    print("\nDUPLICATE ROWS:")
    print(df.duplicated().sum())

    print("\nPRICE STATISTICS:")
    print(df["price"].describe())

    print("\nYEAR RANGE:")
    print(df["year"].min(), "to", df["year"].max())

    print("\nMONTH VALUES:")
    print(sorted(df["month"].unique()))

    print("\nCATEGORIES:")
    print(df["category"].value_counts())

    print("\nInspection complete.")


if __name__ == "__main__":
    inspect_data()