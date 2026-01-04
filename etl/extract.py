import pandas as pd
import logging

def extract_data(heart_rate_path="data/raw_heart_rate.csv", steps_path="data/raw_steps.csv"):
    try:
        hr = pd.read_csv(heart_rate_path)
        steps = pd.read_csv(steps_path)
        logging.info(f"Extracted data from {heart_rate_path} and {steps_path}")
        return hr, steps
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        raise ETLError(f"Missing data file: {e.filename}")
    except pd.errors.ParserError as e:
        logging.error(f"CSV parsing error: {e}")
        raise ETLError("Invalid CSV format")
    except Exception as e:  # Catch-all pentru erori neașteptate
        logging.error(f"Unexpected error during extraction: {e}")
        raise

if __name__ == "__main__":
    print(extract_data())