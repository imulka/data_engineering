import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_csv(
    "output/backtest_results.csv",
    parse_dates=["Date"]
)

plt.figure(figsize=(12, 6))

plt.plot(
    df["Date"],
    df["Buy_Hold_Value"],
    label="SPY Buy & Hold"
)

plt.plot(
    df["Date"],
    df["Strategy_Value"],
    label="200-Day SMA Strategy"
)

plt.title("$100,000 Investment: Buy & Hold vs SMA Strategy")
plt.xlabel("Year")
plt.ylabel("Portfolio Value ($)")
plt.legend()
plt.grid(alpha=0.3)

plt.tight_layout()

plt.savefig(
    "output/portfolio_comparison.png",
    dpi=150
)

print("Saved chart to output/portfolio_comparison.png")

plt.show()