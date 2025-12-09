import pandas as pd
from etl.quality_checks import (
    check_missing_values,
    check_duplicate_rows,
    check_invalid_timestamps,
    check_invalid_numeric,
    check_heart_rate_range,
    check_steps_range,
)


def test_quality_checks_basic():
    df = pd.DataFrame({
        "timestamp": ["2025-01-01", "invalid_date", None],
        "heart_rate": ["80", "not_a_number", "500"],
        "steps": ["10", "-3", None]
    })

    assert check_missing_values(df) > 0
    assert check_duplicate_rows(df) == 0
    assert check_invalid_timestamps(df) == 2
    assert check_invalid_numeric(df, "heart_rate") == 1
    assert check_heart_rate_range(df) == 1  # 500 out of range
    assert check_steps_range(df) == 1       # -3 is invalid
