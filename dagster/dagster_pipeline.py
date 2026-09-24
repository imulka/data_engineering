import pandas as pd
from pathlib import Path

from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    AssetExecutionContext,
    Definitions,
)

from dagster import asset, Definitions, AssetExecutionContext


@asset
def raw_social_media_data(context: AssetExecutionContext):
    """
    Extract raw social media impact data from CSV.
    """
    file_path = "Social_media_impact_on_life.csv"

    df = pd.read_csv(file_path)

    context.log.info(f"Loaded {len(df)} rows")
    context.log.info(f"Columns: {len(df.columns)}")

    return df


@asset
def clean_social_media_data(
    context: AssetExecutionContext,
    raw_social_media_data: pd.DataFrame,
):
    """
    Clean missing values and prepare data for analysis.
    """

    df = raw_social_media_data.copy()

    # Record missing values before cleaning
    missing_stress = df["Perceived_Stress_Score"].isna().sum()
    missing_gpa = df["Academic_Performance_GPA"].isna().sum()

    context.log.info(
        f"Missing stress scores before cleaning: {missing_stress}"
    )
    context.log.info(
        f"Missing GPA values before cleaning: {missing_gpa}"
    )

    # Replace missing numeric values with the column median
    df["Perceived_Stress_Score"] = (
        df["Perceived_Stress_Score"]
        .fillna(df["Perceived_Stress_Score"].median())
    )

    df["Academic_Performance_GPA"] = (
        df["Academic_Performance_GPA"]
        .fillna(df["Academic_Performance_GPA"].median())
    )

    remaining_nulls = int(df.isna().sum().sum())

    context.log.info(f"Rows after cleaning: {len(df)}")
    context.log.info(f"Remaining null values: {remaining_nulls}")

    context.add_output_metadata(
        {
            "rows": len(df),
            "columns": len(df.columns),
            "missing_stress_filled": int(missing_stress),
            "missing_gpa_filled": int(missing_gpa),
            "remaining_nulls": remaining_nulls,
        }
    )

    return df

@asset_check(asset=clean_social_media_data)
def no_missing_values(clean_social_media_data: pd.DataFrame):
    missing = int(clean_social_media_data.isna().sum().sum())

    return AssetCheckResult(
        passed=missing == 0,
        metadata={
            "missing_values": missing,
        },
    )


@asset_check(asset=clean_social_media_data)
def student_id_unique(clean_social_media_data: pd.DataFrame):
    duplicates = int(
        clean_social_media_data["Student_ID"].duplicated().sum()
    )

    return AssetCheckResult(
        passed=duplicates == 0,
        metadata={
            "duplicate_student_ids": duplicates,
        },
    )


@asset_check(asset=clean_social_media_data)
def gpa_valid_range(clean_social_media_data: pd.DataFrame):
    invalid_gpa = int(
        (~clean_social_media_data["Academic_Performance_GPA"]
         .between(0, 4))
        .sum()
    )

    return AssetCheckResult(
        passed=invalid_gpa == 0,
        metadata={
            "invalid_gpa_rows": invalid_gpa,
        },
    )

@asset
def social_media_metrics(
    context: AssetExecutionContext,
    clean_social_media_data: pd.DataFrame,
):
    """
    Aggregate student wellbeing and academic metrics
    by primary social media platform.
    """

    df = clean_social_media_data.copy()

    metrics = (
        df.groupby("Primary_Platform")
        .agg(
            student_count=("Student_ID", "count"),
            avg_daily_usage_hours=("Daily_Usage_Hours", "mean"),
            avg_sleep_hours=("Sleep_Duration_Hours", "mean"),
            avg_stress_score=("Perceived_Stress_Score", "mean"),
            avg_mental_health_index=("Mental_Health_Index", "mean"),
            avg_gpa=("Academic_Performance_GPA", "mean"),
        )
        .reset_index()
    )

    numeric_columns = metrics.select_dtypes(include="number").columns
    metrics[numeric_columns] = metrics[numeric_columns].round(2)

    context.log.info(
        f"Generated metrics for {len(metrics)} social media platforms"
    )

    context.log.info(f"\n{metrics.to_string(index=False)}")

    context.add_output_metadata(
        {
            "platforms_analyzed": len(metrics),
            "total_students": int(metrics["student_count"].sum()),
        }
    )

    return metrics

@asset
def export_pipeline_outputs(
    context: AssetExecutionContext,
    clean_social_media_data: pd.DataFrame,
    social_media_metrics: pd.DataFrame,
):
    """
    Load the final cleaned dataset and analytics metrics
    into CSV files.
    """

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    clean_path = output_dir / "clean_social_media_data.csv"
    metrics_path = output_dir / "social_media_metrics.csv"

    clean_social_media_data.to_csv(clean_path, index=False)
    social_media_metrics.to_csv(metrics_path, index=False)

    context.log.info(
        f"Clean dataset written to: {clean_path}"
    )
    context.log.info(
        f"Metrics written to: {metrics_path}"
    )

    context.add_output_metadata(
        {
            "clean_output": str(clean_path),
            "metrics_output": str(metrics_path),
            "clean_rows": len(clean_social_media_data),
            "metrics_rows": len(social_media_metrics),
        }
    )

    return {
        "clean_output": str(clean_path),
        "metrics_output": str(metrics_path),
    }


defs = Definitions(
    assets=[
        raw_social_media_data,
        clean_social_media_data,
        social_media_metrics,
        export_pipeline_outputs,
    ],
    asset_checks=[
        no_missing_values,
        student_id_unique,
        gpa_valid_range,
    ],
)