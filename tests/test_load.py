import pandas as pd
from etl.load import load_dataframe, init_db, DB_PATH
import sqlite3


def test_load_creates_tables():
    # Create a tiny DataFrame
    df = pd.DataFrame({
        "timestamp": ["2025-01-01 00:00"],
        "heart_rate": [85],
        "hr_rolling_avg": [85]
    })

    init_db()  # ensures DB exists
    load_dataframe(df, "heart_rate")

    # Verify table exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='heart_rate';")
    result = cursor.fetchone()

    conn.close()

    assert result is not None
