import numpy as np
import pandas as pd
import pandera.pandas as pa

from pandera import Check
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


INPUT_FILE = "ml_price_features.csv"
PREDICTIONS_FILE = "regression_predictions.csv"


feature_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(str, nullable=False),
        "year": pa.Column(int, Check.ge(2015), nullable=False),
        "month": pa.Column(int, Check.in_range(1, 12), nullable=False),
        "quarter": pa.Column(int, Check.in_range(1, 4), nullable=False),
        "time_index": pa.Column(int, Check.ge(0), nullable=False),
        "month_sin": pa.Column(float, nullable=False),
        "month_cos": pa.Column(float, nullable=False),
        "item": pa.Column(str, nullable=False),
        "unit": pa.Column(str, nullable=False),
        "category": pa.Column(str, nullable=False),
        "price": pa.Column(float, Check.gt(0), nullable=False),
    },
    strict=True,
    coerce=True,
)


NUMERIC_FEATURES = [
    "year",
    "month",
    "quarter",
    "time_index",
    "month_sin",
    "month_cos",
]

CATEGORICAL_FEATURES = [
    "item",
    "unit",
    "category",
]

TARGET = "price"


def chronological_split(df, train_ratio=0.80):
    dates = sorted(df["date"].unique())

    cutoff_index = int(len(dates) * train_ratio) - 1
    cutoff_date = dates[cutoff_index]

    train_df = df[df["date"] <= cutoff_date].copy()
    test_df = df[df["date"] > cutoff_date].copy()

    return train_df, test_df, cutoff_date


def run_regression():
    print("=" * 60)
    print("PANDERA PIPELINE - STEP 7: RIDGE REGRESSION")
    print("=" * 60)

    df = pd.read_csv(INPUT_FILE)

    print(f"\nRows loaded: {len(df):,}")

    print("\nValidating ML dataset with Pandera...")

    df = feature_schema.validate(
        df,
        lazy=True,
    )

    print("ML dataset validation passed.")

    df["date"] = pd.to_datetime(df["date"])

    train_df, test_df, cutoff_date = chronological_split(df)

    print("\nCHRONOLOGICAL SPLIT")
    print(f"Training cutoff: {cutoff_date.date()}")

    print(
        f"Training range: "
        f"{train_df['date'].min().date()} "
        f"to {train_df['date'].max().date()}"
    )

    print(
        f"Testing range:  "
        f"{test_df['date'].min().date()} "
        f"to {test_df['date'].max().date()}"
    )

    print(f"\nTraining rows: {len(train_df):,}")
    print(f"Testing rows:  {len(test_df):,}")

    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES

    X_train = train_df[features]
    y_train = train_df[TARGET]

    X_test = test_df[features]
    y_test = test_df[TARGET]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                StandardScaler(),
                NUMERIC_FEATURES,
            ),
            (
                "categorical",
                OneHotEncoder(
                    handle_unknown="ignore",
                ),
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "regressor",
                Ridge(alpha=1.0),
            ),
        ]
    )

    print("\nTraining Ridge regression model...")

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(
        y_test,
        predictions,
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_test,
            predictions,
        )
    )

    r2 = r2_score(
        y_test,
        predictions,
    )

    print("\nMODEL RESULTS")
    print("-" * 40)
    print(f"MAE:  ${mae:.4f}")
    print(f"RMSE: ${rmse:.4f}")
    print(f"R²:    {r2:.4f}")

    results = test_df[
        [
            "date",
            "item",
            "category",
            "unit",
            "price",
        ]
    ].copy()

    results["predicted_price"] = predictions

    results["absolute_error"] = (
        results["price"]
        - results["predicted_price"]
    ).abs()

    results = results.rename(
        columns={
            "price": "actual_price",
        }
    )

    results = results.sort_values(
        "absolute_error",
        ascending=False,
    )

    results["date"] = (
        results["date"]
        .dt.strftime("%Y-%m-%d")
    )

    results.to_csv(
        PREDICTIONS_FILE,
        index=False,
    )

    print(
        f"\nPredictions written to: "
        f"{PREDICTIONS_FILE}"
    )

    print("\nLARGEST PREDICTION ERRORS:")
    print(
        results.head(10).to_string(
            index=False
        )
    )

    print("\nSTEP 7 COMPLETE")


if __name__ == "__main__":
    run_regression()