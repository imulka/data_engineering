import pandas as pd


df = pd.read_csv(
    "data/spy.csv",
    parse_dates=["Date"]
)

print("SPY DATA INSPECTION")
print("=" * 40)

print(f"\nRows: {len(df):,}")
print(f"Columns: {len(df.columns)}")

print("\nColumns:")
print(df.columns.tolist())

print("\nDate range:")
print(f"Start: {df['Date'].min()}")
print(f"End:   {df['Date'].max()}")

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicate dates:")
print(df["Date"].duplicated().sum())

print("\nData types:")
print(df.dtypes)