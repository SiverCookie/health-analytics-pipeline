from prefect import flow, task

from etl.extract import extract_data
from etl.transform import clean_heart_rate, clean_steps
from etl.quality_checks import generate_quality_report
from etl.load import load_all


# ---------------- TASK-URI ---------------- #

@task(name="Extract Data")
def extract_task():
    hr_raw, steps_raw = extract_data()
    return hr_raw, steps_raw


@task(name="Transform Data")
def transform_task(hr_raw, steps_raw):
    hr_clean = clean_heart_rate(hr_raw)
    steps_clean = clean_steps(steps_raw)
    return hr_clean, steps_clean


@task(name="Quality Check - RAW")
def quality_raw_task(hr_raw, steps_raw):
    report = generate_quality_report(hr_raw, steps_raw, "quality_raw.json")
    return report


@task(name="Quality Check - CLEAN")
def quality_clean_task(hr_clean, steps_clean):
    report = generate_quality_report(hr_clean, steps_clean, "quality_clean.json")
    return report


@task(name="Load Cleaned Data")
def load_task(hr_clean, steps_clean):
    load_all(hr_clean, steps_clean)
    return "LOAD FINISHED"


# ---------------- FLOW PRINCIPAL ---------------- #

@flow(name="Health Analytics ETL Pipeline")
def etl_flow():
    # 1. Extract
    hr_raw, steps_raw = extract_task()

    # 2. Quality checks pe RAW
    raw_report = quality_raw_task(hr_raw, steps_raw)
    print("RAW Quality:", raw_report)

    # 3. Transform
    hr_clean, steps_clean = transform_task(hr_raw, steps_raw)

    # 4. Quality checks pe CLEAN
    clean_report = quality_clean_task(hr_clean, steps_clean)
    print("CLEAN Quality:", clean_report)

    # 5. Load final
    load_status = load_task(hr_clean, steps_clean)
    print(load_status)


if __name__ == "__main__":
    etl_flow()
