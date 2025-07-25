from fastapi import FastAPI
import requests
import pandas as pd
import numpy as np

app = FastAPI()

@app.get("/run_strategy")
def run_strategy():
    # 1. Obtener datos
    daily = requests.get("http://data_ingestor:8000/daily_data").json()
    intraday = requests.get("http://data_ingestor:8000/intraday_data").json()
    daily_df = pd.DataFrame(daily).dropna()
    intraday_df = pd.DataFrame(intraday)

    # 2. Log-retorno + varianza
    daily_df['Date'] = pd.to_datetime(daily_df['Date'])
    daily_df['log_ret'] = np.log(daily_df['Adj Close']).diff()
    daily_df['variance'] = daily_df['log_ret'].rolling(180).var()

    # 3. Predecir volatilidad GARCH
    predictions = []
    for i in range(180, len(daily_df)):
        window = daily_df['log_ret'].iloc[i-180:i].dropna().tolist()
        res = requests.post("http://garch_model:8001/GARCHService", json={"returns": window}).json()
        predictions.append(res["variance_forecast"])
    daily_df = daily_df.iloc[-len(predictions):].copy()
    daily_df['predictions'] = predictions

    # 4. Premium + señal diaria
    daily_df['prediction_premium'] = (daily_df['predictions'] - daily_df['variance']) / daily_df['variance']
    response = requests.post("http://signal_generator:8002/generate_signals", json={"df": daily_df.to_dict(orient="records")})
    signal_df = pd.DataFrame(response.json())

    # 5. Merge con intradía
    intraday_df['datetime'] = pd.to_datetime(intraday_df['datetime'])
    intraday_df['date'] = intraday_df['datetime'].dt.date
    signal_df['Date'] = pd.to_datetime(signal_df['Date'])
    signal_df['date'] = signal_df['Date'].dt.date
    final_df = intraday_df.merge(signal_df[['date', 'signal_daily']], on='date', how='left')

    # 6. RSI, BBANDS, señal intradía
    import pandas_ta
    final_df['rsi'] = pandas_ta.rsi(final_df['close'], length=20)
    bb = pandas_ta.bbands(final_df['close'], length=20)
    final_df['lband'] = bb.iloc[:, 0]
    final_df['uband'] = bb.iloc[:, 2]
    final_df['signal_intraday'] = final_df.apply(
        lambda x: 1 if x['rsi'] > 70 and x['close'] > x['uband']
        else (-1 if x['rsi'] < 30 and x['close'] < x['lband'] else np.nan), axis=1)

    # 7. Calcular retornos
    result = requests.post("http://return_calculator:8003/calculate", json={"df": final_df.to_dict(orient="records")})
    return result.json()
