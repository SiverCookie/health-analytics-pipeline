import json
from pathlib import Path

from etl.extract import extract_data
from etl.transform import clean_heart_rate, clean_steps
from etl.quality_checks import generate_quality_report
from etl.load import load_all


def run_etl_no_prefect():
    print("=== [1] EXTRACT DATA ===")
    hr_raw, steps_raw = extract_data()
    print(f"Extracted heart_rate rows: {len(hr_raw)}")
    print(f"Extracted steps rows: {len(steps_raw)}")

    # -------------------------------------------------------------------------
    print("\n=== [2] RAW QUALITY CHECK ===")
    raw_quality = generate_quality_report(hr_raw, steps_raw)
    print(json.dumps(raw_quality, indent=4))

    with open("quality_raw.json", "w") as f:
        json.dump(raw_quality, f, indent=4)

    # -------------------------------------------------------------------------
    print("\n=== [3] TRANSFORM ===")
    hr_clean = clean_heart_rate(hr_raw)
    steps_clean = clean_steps(steps_raw)

    print(f"Cleaned heart_rate rows: {len(hr_clean)}")
    print(f"Cleaned steps rows: {len(steps_clean)}")

    # -------------------------------------------------------------------------
    print("\n=== [4] CLEAN QUALITY CHECK ===")
    clean_quality = generate_quality_report(hr_clean, steps_clean)
    print(json.dumps(clean_quality, indent=4))

    with open("quality_clean.json", "w") as f:
        json.dump(clean_quality, f, indent=4)

    # -------------------------------------------------------------------------
    print("\n=== [5] LOAD INTO DATABASE ===")
    load_all(hr_clean, steps_clean)

    print("\n=== LOAD FINISHED ===")


if __name__ == "__main__":
    run_etl_no_prefect()
