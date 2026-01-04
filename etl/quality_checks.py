import pandas as pd
import json
import logging


def check_missing_values(df):
    """Returnează numărul total de valori lipsă."""
    return int(df.isna().sum().sum())


def check_duplicate_rows(df):
    """Returnează numărul de rânduri duplicate."""
    return int(df.duplicated().sum())


def check_invalid_timestamps(df, column="timestamp"):
    """Returnează câte timestamp-uri sunt invalide."""
    temp = pd.to_datetime(df[column], errors="coerce")
    return int(temp.isna().sum())


def check_invalid_numeric(df, column):
    """Returnează câte valori nu pot fi convertite la numeric."""
    temp = pd.to_numeric(df[column], errors="coerce")
    return int(temp.isna().sum())


def check_heart_rate_range(df):
    """Returnează câte valori HR sunt în afara intervalului fiziologic."""
    hr = pd.to_numeric(df["heart_rate"], errors="coerce")
    invalid = (~((hr >= 40) & (hr <= 220))) & (~hr.isna())
    return int(invalid.sum())


def check_steps_range(df):
    """Returnează câte valori steps sunt negative."""
    steps = pd.to_numeric(df["steps"], errors="coerce")
    invalid = (steps < 0) & (~steps.isna())
    return int(invalid.sum())


def generate_quality_report(hr_df, steps_df, output_path="quality_report.json"):
    """Generează un raport calitativ în format JSON."""
    try:
        report = {
            "heart_rate_checks": {
                "missing_values": check_missing_values(hr_df),
                "duplicate_rows": check_duplicate_rows(hr_df),
                "invalid_timestamps": check_invalid_timestamps(hr_df),
               "invalid_numeric_values": check_invalid_numeric(hr_df, "heart_rate"),
                "out_of_range_values": check_heart_rate_range(hr_df),
            },
            "steps_checks": {
                "missing_values": check_missing_values(steps_df),
                "duplicate_rows": check_duplicate_rows(steps_df),
                "invalid_timestamps": check_invalid_timestamps(steps_df),
                "invalid_numeric_values": check_invalid_numeric(steps_df, "steps"),
                "negative_values": check_steps_range(steps_df),
            },
        }

        # Determinăm status-ul final
        total_issues = (
            report["heart_rate_checks"]["missing_values"]
            + report["heart_rate_checks"]["duplicate_rows"]
            + report["heart_rate_checks"]["invalid_timestamps"]
            + report["heart_rate_checks"]["invalid_numeric_values"]
            + report["heart_rate_checks"]["out_of_range_values"]
            + report["steps_checks"]["missing_values"]
            + report["steps_checks"]["duplicate_rows"]
            + report["steps_checks"]["invalid_timestamps"]
            + report["steps_checks"]["invalid_numeric_values"]
            + report["steps_checks"]["negative_values"]
        )

        report["status"] = "PASS" if total_issues == 0 else "FAIL"

        # Scriem în fișier JSON
        with open(output_path, "w") as f:
            json.dump(report, f, indent=4)
        logging.info(f"Quality report generated at {output_path}")
        return report
    except IOError as e:
        logging.error(f"Error writing quality report: {e}")
        raise ETLError("Failed to write quality report")
    except Exception as e:
        logging.error(f"Unexpected error in quality checks: {e}")
        raise