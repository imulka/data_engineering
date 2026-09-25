import pandas as pd


df = pd.read_csv(
    "data/spy.csv",
    parse_dates=["Date"]
)

# Calculate the 200-day simple moving average
df["SMA_200"] = df["Close"].rolling(window=200).mean()

# Create trading signal
df["Signal"] = (df["Close"] > df["SMA_200"]).astype(int)

print("SPY 200-DAY MOVING AVERAGE")
print("=" * 40)

print("\nFirst rows where SMA_200 is available:")
print(
    df[["Date", "Close", "SMA_200", "Signal"]]
    .dropna()
    .head(10)
)

print(f"\nRows with SMA_200: {df['SMA_200'].notna().sum():,}")

# Save dataset with SMA and trading signals
df.to_csv("data/spy_signals.csv", index=False)

print("\nSaved signals to data/spy_signals.csv")