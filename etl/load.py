import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("db/health.db")


def init_db():
    """Create the database file and tables if they don't exist."""
    DB_PATH.parent.mkdir(exist_ok=True)  # create /db folder if missing
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS heart_rate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            heart_rate REAL NOT NULL,
            hr_rolling_avg REAL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            steps INTEGER NOT NULL
        );
    """)

    conn.commit()
    conn.close()


def load_dataframe(df: pd.DataFrame, table_name: str):
    """Load a pandas DataFrame into SQLite, replacing previous data."""
    if not DB_PATH.exists():
        init_db()

    conn = sqlite3.connect(DB_PATH)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"[LOAD] Successfully loaded table: {table_name}")


def load_all(hr_df, steps_df):
    """Load all cleaned tables into database"""
    load_dataframe(hr_df, "heart_rate")
    load_dataframe(steps_df, "steps")
