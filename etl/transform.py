import pandas as pd
import numpy as np

def clean_heart_rate(df):
    df = df.copy()

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Convert heart_rate to numeric
    df["heart_rate"] = pd.to_numeric(df["heart_rate"], errors="coerce")

    # Validate physiological range
    valid_range = (df["heart_rate"] >= 40) & (df["heart_rate"] <= 220)

    # Drop rows that are invalid or missing
    df = df[valid_range]
    df = df.dropna(subset=["timestamp", "heart_rate"])

    # Optional: Add rolling average or daily aggregation
    df["hr_rolling_avg"] = df["heart_rate"].rolling(window=5, min_periods=1).mean()

    return df.reset_index(drop=True)
    
def clean_steps(df):
    df = df.copy()

    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Convert steps to numeric
    df["steps"] = pd.to_numeric(df["steps"], errors="coerce")

    # Remove negative values
    df = df[df["steps"] >= 0]

    # Drop missing timestamp or steps
    df = df.dropna(subset=["timestamp", "steps"])

    # Optional: Aggregate steps per hour (useful for reports)
    df["hour"] = df["timestamp"].dt.floor("h")
    hourly_steps = (
        df.groupby("hour")["steps"].sum().reset_index().rename(columns={"hour": "timestamp"})
    )

    return hourly_steps.reset_index(drop=True)