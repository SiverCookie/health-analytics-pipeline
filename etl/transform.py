import pandas as pd
import numpy as np
import logging  # Adăugat pentru logging – ajută la debugging și monitorizare în producție

# Configurare logging simplu – Vlad folosește comentarii în engleză în cod, așa că păstrăm stilul
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ETLError(Exception):
    """Excepție custom pentru erori în ETL – face mai ușor să identificăm problemele specifice pipeline-ului"""
    pass
    

def clean_heart_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Curăță și transformă datele de heart rate.
    - Convertește timestamp și heart_rate la tipuri corecte
    - Filtrează valori în afara range-ului fiziologic realist (40-220 bpm)
    - Elimină rânduri cu valori lipsă critice
    - Adaugă medie mobilă pe 5 măsurători
    """
    try:
        df = df.copy()  # Evită modificarea DataFrame-ului original
        original_len = len(df)
        
        # Conversii cu coerce – valorile invalide devin NaN
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["heart_rate"] = pd.to_numeric(df["heart_rate"], errors="coerce")
        
        # Filtrare range fiziologic
        valid_range = (df["heart_rate"] >= 40) & (df["heart_rate"] <= 220)
        df = df[valid_range]
        
        # Drop valori critice lipsă
        df = df.dropna(subset=["timestamp", "heart_rate"])
        
        # Rolling average pentru smoothing
        df["hr_rolling_avg"] = df["heart_rate"].rolling(window=5, min_periods=1).mean()
        
        cleaned_len = len(df)
        dropped = original_len - cleaned_len
        
        logging.info(f"Heart rate cleaning complete: {original_len} → {cleaned_len} rows ({dropped} dropped)")
        
        # Verificare prag de pierdere date – dacă pierdem prea mult, semnalăm problemă
        if cleaned_len < original_len * 0.5:  # Mai puțin de 50% date valide
            logging.warning(f"High data loss in heart_rate:{original_len} {dropped} rows dropped ({dropped/original_len:.1%})")
            raise ETLError("Too much invalid data in heart_rate – check source files")
        
        return df.reset_index(drop=True)
    
    except KeyError as e:
        logging.error(f"Missing column in heart_rate data: {e}")
        raise ETLError(f"Required column missing: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during heart_rate cleaning: {e}")
        raise ETLError(f"Heart rate transformation failed: {e}")
        

def clean_steps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Curăță și transformă datele de pași.
    - Convertește timestamp și steps la tipuri corecte
    - Elimină valori negative sau lipsă
    - Agregează pașii la nivel orar (pentru analiză mai relevantă)
    """
    try:
        df = df.copy()
        original_len = len(df)
        
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["steps"] = pd.to_numeric(df["steps"], errors="coerce")
        
        # Pașii nu pot fi negativi
        df = df[df["steps"] >= 0]
        
        # Drop valori critice
        df = df.dropna(subset=["timestamp", "steps"])
        
        # Agregare orară – sumă pași pe oră
        df["hour"] = df["timestamp"].dt.floor("h")
        hourly_steps = (
            df.groupby("hour")["steps"]
            .sum()
            .reset_index()
            .rename(columns={"hour": "timestamp"})
        )
        
        cleaned_len = len(hourly_steps)
        logging.info(f"Steps cleaning & hourly aggregation complete: {original_len} raw rows → {cleaned_len} hourly rows")
        
        # Opțional: avertizare dacă nu avem date deloc după agregare
        if cleaned_len == 0:
            logging.warning("No valid steps data after cleaning and aggregation")
            raise ETLError("No valid steps data remaining after cleaning")
        
        return hourly_steps.reset_index(drop=True)
    
    except KeyError as e:
        logging.error(f"Missing column in steps data: {e}")
        raise ETLError(f"Required column missing: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during steps cleaning: {e}")
        raise ETLError(f"Steps transformation failed: {e}")