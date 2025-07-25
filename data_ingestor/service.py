from fastapi import FastAPI
import pandas as pd
import os

app = FastAPI()

DATA_PATH = "./data/"

@app.get("/daily_data")
def get_daily_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "simulated_daily_data.csv"))
    return df.to_dict(orient="records")

@app.get("/intraday_data")
def get_intraday_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "simulated_5min_data.csv"))
    return df.to_dict(orient="records")
