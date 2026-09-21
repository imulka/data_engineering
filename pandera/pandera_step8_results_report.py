import numpy as np
import pandas as pd

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)


INPUT_FILE = "regression_predictions.csv"
OUTPUT_FILE = "RESULTS.md"


def build_report():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 8: RESULTS REPORT")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nPredictions loaded: {len(df):,}")

    y_true = df["actual_price"]
    y_pred = df["predicted_price"]

    mae = mean_absolute_error(y_true, y_pred)

    rmse = np.sqrt(
        mean_squared_error(y_true, y_pred)
    )

    r2 = r2_score(y_true, y_pred)

    # Category-level errors
    category_errors = (
        df.groupby("category")
        .agg(
            observations=("absolute_error", "size"),
            mae=("absolute_error", "mean"),
        )
        .sort_values("mae", ascending=False)
    )

    # Largest individual errors
    largest_errors = df.nlargest(
        10,
        "absolute_error",
    )[
        [
            "date",
            "item",
            "category",
            "actual_price",
            "predicted_price",
            "absolute_error",
        ]
    ]

    report = f"""# Pandera Data Quality and Regression Pipeline Results

## Project Overview

This project built an end-to-end data validation and machine learning pipeline using Pandas, Pandera, and Scikit-learn.

The source dataset contains monthly U.S. average prices for consumer items from 2015 through 2026.

The pipeline performed:

1. Raw data inspection
2. Pandera schema validation
3. Intentional bad-data testing
4. Cross-column business-rule validation
5. Data transformation
6. Machine-learning feature engineering
7. Chronological train/test splitting
8. Ridge regression
9. Model evaluation

---

## Data Quality Validation

The original dataset contained:

- 8,869 rows
- 8 original columns
- 0 null values
- 0 duplicate rows

Pandera validated:

- Data types
- Required columns
- Non-null constraints
- Year range
- Month range
- Positive prices
- Allowed categories
- Valid dates
- Date/year consistency
- Date/month consistency
- Unique `series_id + date` combinations

An intentional bad-data test inserted five invalid records.

Pandera successfully detected all five violations:

- Invalid month
- Invalid year
- Negative price
- Zero price
- Empty item value

This demonstrated how Pandera can act as a data-quality gate before downstream processing.

---

## Machine Learning Problem

The target variable was:

`price`

The regression model attempted to predict average item prices using:

- Year
- Month
- Quarter
- Time index
- Seasonal sine feature
- Seasonal cosine feature
- Item
- Unit
- Category

Categorical variables were one-hot encoded.

The model used Ridge Regression.

---

## Train/Test Strategy

The dataset was split chronologically rather than randomly.

Training period:

`2015-01-01 through 2024-03-01`

Testing period:

`2024-04-01 through 2026-07-01`

Training rows:

`7,160`

Testing rows:

`1,709`

This approach better simulates predicting future prices from historical observations.

---

## Model Results

| Metric | Result |
|---|---:|
| MAE | ${mae:.4f} |
| RMSE | ${rmse:.4f} |
| R² | {r2:.4f} |

### Interpretation

**MAE = ${mae:.2f}**

On average, the model's predicted price differed from the actual price by approximately ${mae:.2f}.

**RMSE = ${rmse:.2f}**

The RMSE is higher than the MAE because several observations had substantially larger prediction errors. RMSE penalizes these large errors more heavily.

**R² = {r2:.4f}**

The model explains approximately {r2 * 100:.2f}% of the variation in prices in the future test dataset.

This is a strong baseline result, although it does not mean the model predicts every individual item accurately.

---

## Largest Prediction Errors

{largest_errors.to_markdown(index=False)}

---

## Error by Category

{category_errors.to_markdown()}

---

## Key Findings

The Ridge regression model performed well overall, explaining approximately {r2 * 100:.1f}% of price variation.

The average prediction error was approximately ${mae:.2f}.

The largest errors were concentrated in specific products, particularly high-priced beef products such as sirloin steak.

This suggests that some individual product prices may experience nonlinear price movements that a linear Ridge regression model cannot fully capture.

A future model such as Random Forest, Gradient Boosting, or XGBoost could potentially capture these nonlinear relationships better.

---

## Data Quality Observation

The prediction results also exposed an important limitation of schema validation.

One record showed:

`Coffee, 100%, ground roast, all sizes`

with the category:

`Beef`

Pandera did not reject this record because `Beef` is technically an allowed category.

This demonstrates the difference between:

- structural validation
- business-rule validation
- semantic validation

Additional item-to-category consistency rules could be added to the pipeline if authoritative mappings are available.

---

## Conclusion

This project demonstrates how Pandera can be integrated into an ETL and machine-learning workflow to prevent invalid data from silently reaching downstream models.

The workflow validated data before transformation, deliberately tested schema failures, enforced business rules, engineered features, trained a regression model, and evaluated predictions using future observations.

Final model performance:

- MAE: ${mae:.4f}
- RMSE: ${rmse:.4f}
- R²: {r2:.4f}
"""

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(report)

    print("\nMODEL METRICS")
    print(f"MAE:  ${mae:.4f}")
    print(f"RMSE: ${rmse:.4f}")
    print(f"R²:    {r2:.4f}")

    print(f"\nReport written to: {OUTPUT_FILE}")
    print("\nSTEP 8 COMPLETE")


if __name__ == "__main__":
    build_report()