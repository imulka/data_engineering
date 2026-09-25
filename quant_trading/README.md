# Quantitative Trading Pipeline — SPY Buy & Hold vs. 200-Day SMA

A small quantitative trading and backtesting project built with Python.

The project compares two strategies using historical SPY data:

1. **Buy and Hold SPY**
2. **200-Day Simple Moving Average (SMA) Strategy**

The purpose of the exercise is to learn the basic structure of a quantitative trading pipeline:

```text
Market Data
    ↓
Data Inspection
    ↓
Technical Indicator
    ↓
Trading Signal
    ↓
Backtesting
    ↓
Portfolio Simulation
    ↓
Risk / Return Analysis
    ↓
Visualization
```

The project runs locally and does **not require a paid API, API key, brokerage account, or QuantConnect subscription**.

---

## 1. Project Structure

```text
quant_trading/
│
├── 01_download_data.py
├── 02_inspect_data.py
├── 03_build_signals.py
├── 04_backtest.py
├── 05_visualize_results.py
├── requirements.txt
├── README.md
│
├── data/
│   ├── spy.csv
│   └── spy_signals.csv
│
└── output/
    ├── backtest_results.csv
    └── portfolio_comparison.png
```

---

# 2. Environment Setup

Create a Python virtual environment:

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

The main libraries used are:

- `pandas`
- `numpy`
- `matplotlib`
- `yfinance`

Check that the virtual environment is active:

```bash
which python
which pip
```

They should point to:

```text
quant_trading/.venv/bin/python
quant_trading/.venv/bin/pip
```

---

# 3. Step 1 — Download Historical SPY Data

Run:

```bash
python 01_download_data.py
```

The script downloads historical SPY market data from 2005 through 2025 using `yfinance`.

The data includes:

```text
Date
Close
High
Low
Open
Volume
```

Adjusted prices are used:

```python
auto_adjust=True
```

This accounts for events such as dividends and stock splits when constructing the historical price series.

The script also flattens the MultiIndex columns returned by newer versions of `yfinance`.

Output:

```text
data/spy.csv
```

During this exercise we downloaded:

```text
5,283 trading days
```

covering:

```text
2005-01-03 through 2025-12-31
```

---

# 4. Step 2 — Inspect the Dataset

Run:

```bash
python 02_inspect_data.py
```

This performs basic data-quality checks before building a trading strategy.

We check:

- number of rows
- column names
- date range
- missing values
- duplicate dates
- data types

Our dataset contained:

```text
Rows: 5,283
Missing values: 0
Duplicate dates: 0
```

This step is important because trading algorithms should not blindly operate on unvalidated data.

---

# 5. Step 3 — Build the Trading Signal

Run:

```bash
python 03_build_signals.py
```

The strategy uses a **200-day Simple Moving Average**.

The SMA is:

```text
SMA(200) = average closing price over the previous 200 trading days
```

In Python:

```python
df["SMA_200"] = df["Close"].rolling(window=200).mean()
```

The trading rule is:

```text
Close > SMA_200  → Signal = 1
Close <= SMA_200 → Signal = 0
```

Interpretation:

```text
Signal = 1 → Invest in SPY
Signal = 0 → Stay in cash
```

Example:

```text
Close = 81.61
SMA   = 81.21

81.61 > 81.21

Signal = 1
```

The first 200-day SMA becomes available after approximately 200 trading observations.

The resulting dataset is saved as:

```text
data/spy_signals.csv
```

---

# 6. Avoiding Look-Ahead Bias

One of the most important concepts in the exercise is avoiding **look-ahead bias**.

The strategy calculates a signal using today's closing price.

But today's closing price is not known until the trading day has ended.

Therefore we cannot use today's signal to claim that we captured today's return.

Instead:

```text
Today's market data
        ↓
Generate signal
        ↓
Shift signal one day
        ↓
Tomorrow's position
```

Implemented with:

```python
df["Position"] = df["Signal"].shift(1).fillna(0)
```

This means:

```text
Signal today → Position tomorrow
```

Without this shift, the backtest would effectively allow the algorithm to trade using information from the future.

---

# 7. Step 4 — Backtest

Run:

```bash
python 04_backtest.py
```

Starting capital:

```text
$100,000
```

Two portfolios are simulated.

## Portfolio A — Buy & Hold

The investor owns SPY continuously.

Daily return:

```python
df["Market_Return"] = df["Close"].pct_change()
```

## Portfolio B — SMA Strategy

The portfolio receives SPY's return only while the algorithm is invested.

```python
df["Strategy_Return"] = (
    df["Position"] * df["Market_Return"]
)
```

When:

```text
Position = 1
```

the strategy participates in SPY returns.

When:

```text
Position = 0
```

the strategy remains in cash.

For this introductory experiment, cash earns **0%**.

---

# 8. Portfolio Compounding

Portfolio values are calculated using compounded daily returns.

Conceptually:

```text
New Portfolio Value
=
Previous Portfolio Value × (1 + Daily Return)
```

or:

```text
V(t) = V(t-1) × (1 + r)
```

In pandas:

```python
initial_capital = 100_000

df["Buy_Hold_Value"] = (
    initial_capital
    * (1 + df["Market_Return"].fillna(0)).cumprod()
)

df["Strategy_Value"] = (
    initial_capital
    * (1 + df["Strategy_Return"].fillna(0)).cumprod()
)
```

---

# 9. Backtest Results

For the historical period tested, $100,000 produced approximately:

```text
Buy & Hold:    $833,743
SMA Strategy:  $545,843
```

Buy and hold therefore produced substantially greater ending wealth.

However, total return does not measure the amount of risk experienced along the way.

---

# 10. CAGR

CAGR means:

**Compound Annual Growth Rate**

It answers:

> What constant annual compounded return would produce the same beginning and ending portfolio values?

Formula:

```text
CAGR =
(Ending Value / Beginning Value)^(1 / Years) - 1
```

Results:

```text
Buy & Hold:    11.07%
SMA Strategy:   8.76%
```

Buy and hold generated the higher annualized return.

---

# 11. Maximum Drawdown

Maximum drawdown measures the largest historical decline from a previous portfolio peak.

Example:

```text
Portfolio peak: $100,000
Portfolio falls: $60,000

Drawdown = -40%
```

Our results:

```text
Buy & Hold:    -55.19%
SMA Strategy:  -20.68%
```

This was one of the most important findings of the exercise.

Although the SMA strategy generated less wealth, its largest historical decline was dramatically smaller.

---

# 12. Annualized Volatility

Volatility measures the variability of returns.

Daily volatility is converted to annualized volatility using approximately 252 U.S. trading days:

```text
Annual Volatility
=
Daily Standard Deviation × sqrt(252)
```

Results:

```text
Buy & Hold:    19.32%
SMA Strategy:  11.63%
```

The SMA strategy therefore experienced substantially lower historical volatility.

---

# 13. Sharpe Ratio

The Sharpe ratio measures return relative to volatility.

For this introductory exercise, we assumed a:

```text
0% risk-free rate
```

The annualized calculation was:

```python
Sharpe =
    mean_daily_return
    / daily_return_standard_deviation
    * sqrt(252)
```

Results:

```text
Buy & Hold:    0.64
SMA Strategy:  0.78
```

Under this simplified calculation, the SMA strategy produced a higher historical return per unit of volatility despite producing lower total wealth.

---

# 14. Overall Comparison

| Metric                | Buy & Hold | 200-Day SMA |
| --------------------- | ---------: | ----------: |
| Starting Capital      |   $100,000 |    $100,000 |
| Ending Value          |   $833,743 |    $545,843 |
| CAGR                  |     11.07% |       8.76% |
| Annualized Volatility |     19.32% |      11.63% |
| Maximum Drawdown      |    -55.19% |     -20.68% |
| Sharpe Ratio          |       0.64 |        0.78 |

The experiment demonstrates an important quantitative-finance concept:

**Maximum return and risk-adjusted performance are not necessarily the same objective.**

Buy and hold generated substantially more wealth during this historical period.

The SMA strategy generated less wealth but experienced lower volatility, a much smaller maximum drawdown, and a higher Sharpe ratio under the assumptions used in this exercise.

---

# 15. Step 5 — Visualize the Results

Run:

```bash
python 05_visualize_results.py
```

The script plots the growth of $100,000 under both strategies.

Output:

```text
output/portfolio_comparison.png
```

The graph makes the behavior of the strategies easier to understand.

During major market declines, the SMA strategy can move into cash, causing its portfolio line to become relatively flat.

During strong bull markets, however, exiting SPY can cause the strategy to miss gains.

This trade-off accumulates significantly over long periods.

---

# 16. Run the Entire Pipeline Again

Activate the environment:

```bash
source .venv/bin/activate
```

Then run:

```bash
python 01_download_data.py
python 02_inspect_data.py
python 03_build_signals.py
python 04_backtest.py
python 05_visualize_results.py
```

Pipeline:

```text
01_download_data.py
        ↓
data/spy.csv
        ↓
02_inspect_data.py
        ↓
03_build_signals.py
        ↓
data/spy_signals.csv
        ↓
04_backtest.py
        ↓
output/backtest_results.csv
        ↓
05_visualize_results.py
        ↓
output/portfolio_comparison.png
```

---

# 17. Important Limitations

This is an educational backtest, not a production trading system.

The current model does not yet account for:

- transaction costs
- bid/ask spreads
- slippage
- taxes
- realistic execution timing
- interest earned while holding cash
- changing risk-free rates
- market impact
- portfolio allocation constraints

The strategy has also been evaluated historically on the same broad dataset used to examine its behavior.

A more rigorous quantitative research process would include:

- training/in-sample periods
- out-of-sample testing
- walk-forward analysis
- transaction-cost modeling
- parameter sensitivity testing
- comparison against additional benchmarks

---

# 18. Key Concepts Learned

This exercise introduced:

- historical market-data ingestion
- financial time-series data
- adjusted prices
- moving averages
- algorithmic trading signals
- position management
- look-ahead bias
- daily returns
- compounding
- backtesting
- CAGR
- volatility
- maximum drawdown
- Sharpe ratio
- risk-adjusted performance
- strategy benchmarking
- portfolio visualization

---

# 19. Possible Next Experiments

The pipeline can later be extended with strategies such as:

```text
50-day / 200-day moving-average crossover

Momentum strategy

Volatility targeting

RSI strategy

MACD strategy

Multi-asset portfolio

SPY + Treasury strategy

Risk-parity portfolio

Machine-learning signals
```

A particularly useful next experiment would be to compare:

```text
100% SPY

vs.

100% SMA Strategy

vs.

70% SPY + 30% SMA Strategy
```

This would demonstrate whether combining an algorithmic strategy with passive investing can improve portfolio diversification and risk-adjusted performance.

---

# Summary

This project built a complete small quantitative-trading research pipeline from scratch using Python.

The experiment showed that from approximately 2005–2025:

```text
Buy & Hold
    → higher absolute return
    → larger drawdowns
    → higher volatility

200-Day SMA
    → lower absolute return
    → smaller drawdowns
    → lower volatility
    → higher simplified Sharpe ratio
```

The central lesson is:

> A quantitative strategy does not necessarily need to produce the highest raw return to change the risk characteristics of a portfolio.

The next stage of quantitative research is determining whether those characteristics remain robust after realistic costs, alternative time periods, and out-of-sample testing.
