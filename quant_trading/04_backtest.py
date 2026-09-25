import pandas as pd


df = pd.read_csv(
    "data/spy_signals.csv",
    parse_dates=["Date"]
)

# Remove the initial period where the 200-day SMA does not exist
df = df.dropna(subset=["SMA_200"]).copy()

# Daily return from simply owning SPY
df["Market_Return"] = df["Close"].pct_change()

# Use yesterday's signal for today's position.
# This prevents us from using today's closing price to trade retroactively.
df["Position"] = df["Signal"].shift(1).fillna(0)

# Strategy earns the market return only when Position = 1
df["Strategy_Return"] = df["Position"] * df["Market_Return"]

print("BACKTEST")
print("=" * 40)

print(
    df[
        [
            "Date",
            "Close",
            "SMA_200",
            "Signal",
            "Position",
            "Market_Return",
            "Strategy_Return",
        ]
    ].head(10)
)

# Starting capital
initial_capital = 100_000

# Grow $100,000 using each strategy's daily returns
df["Buy_Hold_Value"] = initial_capital * (1 + df["Market_Return"].fillna(0)).cumprod()

df["Strategy_Value"] = initial_capital * (1 + df["Strategy_Return"].fillna(0)).cumprod()

print("\nFINAL PORTFOLIO VALUES")
print("=" * 40)

print(f"Buy & Hold:  ${df['Buy_Hold_Value'].iloc[-1]:,.2f}")
print(f"SMA Strategy: ${df['Strategy_Value'].iloc[-1]:,.2f}")

# Calculate maximum drawdown
df["Buy_Hold_Peak"] = df["Buy_Hold_Value"].cummax()
df["Buy_Hold_Drawdown"] = (
    df["Buy_Hold_Value"] / df["Buy_Hold_Peak"] - 1
)

df["Strategy_Peak"] = df["Strategy_Value"].cummax()
df["Strategy_Drawdown"] = (
    df["Strategy_Value"] / df["Strategy_Peak"] - 1
)

print("\nMAXIMUM DRAWDOWN")
print("=" * 40)

print(f"Buy & Hold:   {df['Buy_Hold_Drawdown'].min():.2%}")
print(f"SMA Strategy: {df['Strategy_Drawdown'].min():.2%}")

# Calculate CAGR
years = (df["Date"].iloc[-1] - df["Date"].iloc[0]).days / 365.25

buy_hold_cagr = (
    df["Buy_Hold_Value"].iloc[-1] / initial_capital
) ** (1 / years) - 1

strategy_cagr = (
    df["Strategy_Value"].iloc[-1] / initial_capital
) ** (1 / years) - 1

print("\nCAGR")
print("=" * 40)

print(f"Buy & Hold:   {buy_hold_cagr:.2%}")
print(f"SMA Strategy: {strategy_cagr:.2%}")


# Calculate annualized volatility
trading_days = 252

buy_hold_volatility = df["Market_Return"].std() * (trading_days ** 0.5)

strategy_volatility = df["Strategy_Return"].std() * (trading_days ** 0.5)

print("\nANNUALIZED VOLATILITY")
print("=" * 40)

print(f"Buy & Hold:   {buy_hold_volatility:.2%}")
print(f"SMA Strategy: {strategy_volatility:.2%}")

# Calculate annualized Sharpe ratio
# For this exercise, assume a 0% risk-free rate.
buy_hold_sharpe = (
    df["Market_Return"].mean() / df["Market_Return"].std()
) * (trading_days ** 0.5)

strategy_sharpe = (
    df["Strategy_Return"].mean() / df["Strategy_Return"].std()
) * (trading_days ** 0.5)

print("\nSHARPE RATIO")
print("=" * 40)

print(f"Buy & Hold:   {buy_hold_sharpe:.2f}")
print(f"SMA Strategy: {strategy_sharpe:.2f}")

# Save complete backtest results
df.to_csv("output/backtest_results.csv", index=False)

print("\nSaved backtest results to output/backtest_results.csv")