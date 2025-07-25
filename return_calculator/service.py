from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
import numpy as np

app = FastAPI()

class ReturnInput(BaseModel):
    df: List[dict]

@app.post("/calculate")
def calculate_returns(input: ReturnInput):
    df = pd.DataFrame(input.df)
    df['return'] = np.log(df['close']).diff()
    df['forward_return'] = df['return'].shift(-1)

    df['return_sign'] = df.apply(
        lambda x: -1 if (x['signal_daily'] == 1 and x['signal_intraday'] == 1)
        else (1 if (x['signal_daily'] == -1 and x['signal_intraday'] == -1) else np.nan),
        axis=1
    )

    df['return_sign'] = df.groupby(pd.to_datetime(df['datetime']).dt.date)['return_sign'].transform('ffill')
    df['strategy_return'] = df['forward_return'] * df['return_sign']
    df['date'] = pd.to_datetime(df['datetime']).dt.date
    daily_returns = df.groupby('date')['strategy_return'].sum().reset_index()

    return daily_returns.to_dict(orient='records')
