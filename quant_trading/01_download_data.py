import yfinance as yf


ticker = "SPY"

print(f"Downloading historical data for {ticker}...")

df = yf.download(
    ticker,
    start="2005-01-01",
    end="2026-01-01",
    auto_adjust=True,
    progress=False
)

# Flatten yfinance's MultiIndex columns
df.columns = df.columns.get_level_values(0)

# Give the index a clean name
df.index.name = "Date"

print(f"Rows downloaded: {len(df)}")
print("\nFirst 5 rows:")
print(df.head())

df.to_csv("data/spy.csv")

print("\nSaved clean data to data/spy.csv")