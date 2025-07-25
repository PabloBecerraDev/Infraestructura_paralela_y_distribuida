from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import numpy as np
import pandas as pd
import pandas_ta

app = FastAPI()

class SignalInput(BaseModel):
    df: List[dict]

@app.post("/generate_signals")
def generate_signals(input: SignalInput):
    df = pd.DataFrame(input.df)

    # Señal diaria (ya precalculada: pred, var, premium)
    df['premium_std'] = df['prediction_premium'].rolling(180).std()
    df['signal_daily'] = df.apply(
        lambda x: 1 if x['prediction_premium'] > x['premium_std']
        else (-1 if x['prediction_premium'] < -x['premium_std'] else np.nan), axis=1)
    df['signal_daily'] = df['signal_daily'].shift()

    return df[['Date', 'signal_daily']].dropna().to_dict(orient='records')
