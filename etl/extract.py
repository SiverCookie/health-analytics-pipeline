import pandas as pd

def extract_data():
    hr = pd.read_csv("data/raw_heart_rate.csv")
    steps = pd.read_csv("data/raw_steps.csv")
    return hr, steps

if __name__ == "__main__":
    print(extract_data())