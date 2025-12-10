📌 Health Analytics ETL Pipeline
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Platform](https://img.shields.io/badge/Platform-Windows%2010%2F11-lightgrey)
![Docker](https://img.shields.io/badge/Docker-Ready-brightgreen)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect%203-blueviolet)
![CI/CD](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-success)
![Tests](https://img.shields.io/badge/Tests-Pytest-green)
![Status](https://img.shields.io/badge/Build-Passing-brightgreen)

A Python-based data engineering pipeline for cleaning, validating, and loading simulated wearable device data.

🧩 Overview

This project implements a complete ETL pipeline for processing synthetic health data (heart rate & step count) typically collected from wearable devices.

The pipeline includes:

Data Extraction (CSV-based raw ingestion)

Data Cleaning & Validation (timestamp parsing, numeric checks, anomaly filtering)

Data Quality Reporting (before & after cleaning)

ETL Orchestration with Prefect

Automated Tests (pytest)

CI/CD (GitHub Actions)

Dockerized execution for reproducibility

💡 All development and testing were performed on Windows 10/11.
The project runs fully on Windows, both locally and inside Docker Desktop.

🏗 Project Architecture

health-analytics-pipeline/
│
├── etl/
│   ├── extract.py              # Load raw CSV data
│   ├── transform.py            # Clean heart_rate and steps
│   ├── quality_checks.py       # Validation rules + quality scoring
│   ├── load.py                 # Load cleaned data into SQLite
│   ├── pipeline.py             # Prefect 3 orchestration flow
│   ├── run_local_no_prefect.py # Standalone ETL execution (used in Docker)
│   └── __init__.py
│
├── tests/                      # pytest test suite
│
├── raw_data/                   # Sample .csv data
│
├── db/                         # SQLite database (generated)
│
├── Dockerfile
├── requirements.txt
├── pytest.ini
└── README.md

🖼 ETL Flow Diagram

        +-----------------+
        |     Extract     |
        | (CSV → DataFrame)
        +--------+--------+
                 |
                 v
        +-----------------+
        | Raw Quality     |
        | Checks          |
        +--------+--------+
                 |
                 v
        +-----------------+
        |   Transform     |
        | (Cleaning & Fix)|
        +--------+--------+
                 |
                 v
        +-----------------+
        | Clean Quality   |
        | Checks          |
        +--------+--------+
                 |
                 v
        +-----------------+
        |      Load       |
        |   (SQLite)      |
        +-----------------+

🚀 Features
✔ 1. Realistic ETL Logic

timestamp corrections

invalid numeric filtering

heart rate physiological range checks

negative/invalid step counts

duplicate row detection

✔ 2. Automated Data Quality Reports

Before cleaning → After cleaning
Saved as:

quality_raw.json
quality_clean.json

✔ 3. SQLite Loading

Tables created automatically:

heart_rate

steps

✔ 4. Dockerized Execution

Completely reproducible ETL run using:

docker build -t health-etl .
docker run --rm health-etl

✔ 5. Windows-first development

Everything runs natively on Windows:

Python

Prefect

Docker Desktop

SQLite

pytest

✔ 6. CI/CD Pipeline

GitHub Actions automatically runs:

install deps

run test suite

report results

🧪 Running the ETL Pipeline Locally (Windows)
1️⃣ Create virtual environment
python -m venv venv
venv\Scripts\activate

2️⃣ Install dependencies
pip install -r requirements.txt

3️⃣ Run the ETL using Prefect (full orchestration)
python etl/pipeline.py

4️⃣ Run unit tests
pytest

🐳 Running the ETL in Docker (Recommended)
1️⃣ Build the image
docker build -t health-etl .

2️⃣ Run the ETL
docker run --rm health-etl


Inside Docker, the ETL is executed via:

etl/run_local_no_prefect.py


This script runs without Prefect’s orchestration engine, ensuring stable execution inside containers.

🗄 Database Output

After running the pipeline (local or Docker):

db/
└── health.db


Tables:

heart_rate

steps

You can inspect the database using tools like DB Browser for SQLite.

🎯 Why This Project Matters (Recruiter-Friendly Summary)

This project demonstrates:

🔹 Real-world ETL engineering skills

Handling messy health data and implementing cleaning & validation steps.

🔹 Knowledge of modern orchestration tools

Prefect 3 used for workflow orchestration.

🔹 CI/CD exposure

GitHub Actions pipeline for automated testing.

🔹 Software engineering best practices

Modular code, testing, version control, and documentation.

🔹 Docker proficiency

Containerized ETL pipeline suitable for production-like workflows.

🔹 Python proficiency

Pandas, NumPy, data validation, file handling, SQLite integration.

📬 Contact

Vlad-Petru Opriș
(www.linkedin.com/in/vlad-opris-194171196)

🏁 Final Notes

This project was:

developed entirely on Windows

tested on Windows + Docker Desktop (Linux containers)

validated via automated CI/CD

It is designed as a portfolio-grade example of Python-based automation, ETL engineering, and modern workflow orchestration.