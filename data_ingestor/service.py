from fastapi import FastAPI
import pandas as pd
import numpy as np
import os

app = FastAPI()

DATA_PATH = "data"

@app.get("/daily_data")
def get_daily_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "simulated_daily_data.csv"))
    df = df.dropna(axis=1, how='all')  # 🔧 elimina columnas completamente vacías
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    return df.to_dict(orient="records")

@app.get("/intraday_data")
def get_intraday_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "simulated_5min_data.csv"))
    df = df.dropna(axis=1, how='all')  # 🔧 elimina columnas completamente vacías
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    return df.to_dict(orient="records")
