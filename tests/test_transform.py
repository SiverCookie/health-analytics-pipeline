import pandas as pd
from etl.transform import clean_heart_rate, clean_steps


def test_clean_heart_rate_removes_invalid_and_outliers():
    df = pd.DataFrame({
        "timestamp": ["2025-01-01 00:00", "invalid_date", "2025-01-01 00:02", "2025-01-01 00:04", "2025-01-05 00:00"],
        "heart_rate": ["80", "not_a_number", "300", "100", "110"]  # 300 is out of range
    })

    cleaned = clean_heart_rate(df)

    # Only the first row should remain
    assert len(cleaned) == 3
    assert cleaned.iloc[0]["heart_rate"] == 80


def test_clean_steps_removes_negative_and_invalid():
    df = pd.DataFrame({
        "timestamp": ["2025-01-01 01:00", None, "2025-01-01 02:00", "2025-01-01 03:00", "2025-01-01 04:00"],
        "steps": ["10", "-5", "not_a_number", "15", "10"]
    })

    cleaned = clean_steps(df)

    # only first row -> aggregated
    assert len(cleaned) == 3
    assert cleaned.iloc[0]["steps"] == 10
