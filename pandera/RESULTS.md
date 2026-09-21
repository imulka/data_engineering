# Pandera Data Quality and Regression Pipeline Results

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
| MAE | $0.6832 |
| RMSE | $0.9820 |
| R² | 0.8962 |

### Interpretation

**MAE = $0.68**

On average, the model's predicted price differed from the actual price by approximately $0.68.

**RMSE = $0.98**

The RMSE is higher than the MAE because several observations had substantially larger prediction errors. RMSE penalizes these large errors more heavily.

**R² = 0.8962**

The model explains approximately 89.62% of the variation in prices in the future test dataset.

This is a strong baseline result, although it does not mean the model predicts every individual item accurately.

---

## Largest Prediction Errors

| date       | item                                  | category   |   actual_price |   predicted_price |   absolute_error |
|:-----------|:--------------------------------------|:-----------|---------------:|------------------:|-----------------:|
| 2026-04-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.727 |          10.048   |          4.67895 |
| 2026-07-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.592 |          10.092   |          4.49996 |
| 2026-06-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.451 |          10.0784  |          4.37264 |
| 2025-08-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.319 |           9.9694  |          4.3496  |
| 2026-05-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.274 |          10.0653  |          4.2087  |
| 2026-02-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.191 |          10.0071  |          4.18388 |
| 2025-09-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.135 |           9.96437 |          4.17063 |
| 2026-03-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.124 |          10.0229  |          4.10106 |
| 2025-12-01 | Steak, sirloin, USDA Choice, boneless | Beef       |         14.027 |           9.95503 |          4.07197 |
| 2026-04-01 | Coffee, 100%, ground roast, all sizes | Beef       |          9.723 |           5.76252 |          3.96048 |

---

## Error by Category

| category             |   observations |      mae |
|:---------------------|---------------:|---------:|
| Beef                 |            316 | 1.64301  |
| Meat, broad category |             81 | 1.09468  |
| Eggs                 |             27 | 0.988713 |
| Fruit                |            157 | 0.736552 |
| Vegetables           |            184 | 0.551723 |
| Bakery and grains    |            160 | 0.452022 |
| Pantry and snacks    |             27 | 0.398606 |
| Drinks               |            108 | 0.390227 |
| Energy               |            219 | 0.373314 |
| Poultry              |             81 | 0.300237 |
| Dairy and fats       |            189 | 0.267594 |
| Pork and deli        |            160 | 0.212637 |

---

## Key Findings

The Ridge regression model performed well overall, explaining approximately 89.6% of price variation.

The average prediction error was approximately $0.68.

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

- MAE: $0.6832
- RMSE: $0.9820
- R²: 0.8962
