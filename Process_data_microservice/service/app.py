import ray
import pandas as pd
import numpy as np
import pandas_ta
import requests
import time
from flask import Flask, jsonify, Response


app = Flask(__name__)


@app.route('/status', methods=["GET"])
def status():
    """Endpoint simple de status"""
    return jsonify({
        "status": "OK",
        "service": "Financial Data Processor",
        "version": "1.0.0",
        "ray_initialized": ray.is_initialized()
    })


@app.route('/getData', methods=["POST"])
def getDataByotherService():
    try:
        response = requests.post("http://servicio_get_data:5000/sp500_data")
        response.raise_for_status()

        # Convertir el JSON a DataFrame
        data = pd.DataFrame(response.json())

        # Validación de columnas
        if 'date' in data.columns and 'ticker' in data.columns:
            data = data.set_index(['date', 'ticker'])
            data.index.names = ['date', 'ticker']

        # Estandarizar nombres de columnas
        data.columns = data.columns.str.lower()

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        data = calculateIndicators(data, num_blocks=4)

        data['dollar_volume'] = (data.loc[:, 'dollar_volume'].unstack('ticker').rolling(5*12, min_periods=12).mean().stack())

        data['dollar_vol_rank'] = (data.groupby('date')['dollar_volume'].rank(ascending=False))

        data = data[data['dollar_vol_rank']<150].drop(['dollar_volume', 'dollar_vol_rank'], axis=1)

        print(data)

        # Convertir a JSON manualmente
        json_data = data.reset_index().to_json(orient="records")  
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route('/getDataSequential', methods=["POST"])
def getDataSequential():
    """Endpoint que procesa los datos de manera secuencial (no paralela)"""
    try:
        response = requests.post("http://servicio_get_data:5000/sp500_data")
        response.raise_for_status()

        # Convertir el JSON a DataFrame
        data = pd.DataFrame(response.json())

        # Validación de columnas
        if 'date' in data.columns and 'ticker' in data.columns:
            data = data.set_index(['date', 'ticker'])
            data.index.names = ['date', 'ticker']

        # Estandarizar nombres de columnas
        data.columns = data.columns.str.lower()

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Usar la versión secuencial
        data = calculateIndicatorsSequential(data)

        data['dollar_volume'] = (data.loc[:, 'dollar_volume'].unstack('ticker').rolling(5*12, min_periods=12).mean().stack())

        data['dollar_vol_rank'] = (data.groupby('date')['dollar_volume'].rank(ascending=False))

        data = data[data['dollar_vol_rank']<150].drop(['dollar_volume', 'dollar_vol_rank'], axis=1)

        print(data)

        # Convertir a JSON manualmente
        json_data = data.reset_index().to_json(orient="records")  
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route('/comparePerformance', methods=["POST"])
def comparePerformance():
    """Endpoint que compara el tiempo de procesamiento entre versión paralela y secuencial"""
    try:
        response = requests.post("http://servicio_get_data:5000/sp500_data")
        response.raise_for_status()

        # Convertir el JSON a DataFrame
        data = pd.DataFrame(response.json())

        # Validación de columnas
        if 'date' in data.columns and 'ticker' in data.columns:
            data = data.set_index(['date', 'ticker'])
            data.index.names = ['date', 'ticker']

        # Estandarizar nombres de columnas
        data.columns = data.columns.str.lower()

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Hacer una copia para cada test
        data_parallel = data.copy()
        data_sequential = data.copy()

        # Medir tiempo de procesamiento paralelo
        start_time_parallel = time.time()
        result_parallel = calculateIndicators(data_parallel, num_blocks=4)
        end_time_parallel = time.time()
        parallel_time = end_time_parallel - start_time_parallel

        # Medir tiempo de procesamiento secuencial
        start_time_sequential = time.time()
        result_sequential = calculateIndicatorsSequential(data_sequential)
        end_time_sequential = time.time()
        sequential_time = end_time_sequential - start_time_sequential

        # Calcular mejora de rendimiento
        speedup = sequential_time / parallel_time if parallel_time > 0 else 0
        improvement_percentage = ((sequential_time - parallel_time) / sequential_time * 100) if sequential_time > 0 else 0

        return jsonify({
            "performance_comparison": {
                "parallel_processing": {
                    "time_seconds": round(parallel_time, 4),
                    "method": "Ray parallel processing"
                },
                "sequential_processing": {
                    "time_seconds": round(sequential_time, 4),
                    "method": "Sequential processing"
                },
                "performance_metrics": {
                    "speedup_factor": round(speedup, 2),
                    "improvement_percentage": round(improvement_percentage, 2),
                    "data_rows_processed": len(result_parallel),
                    "unique_tickers": len(data.index.get_level_values(1).unique())
                }
            }
        })

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


def calculateIndicatorsSequential(df):
    """
    Versión secuencial de calculateIndicators (sin paralelización)
    Produce exactamente el mismo resultado que la versión paralela
    """
    # Crear una copia para no modificar el original
    df = df.copy()
    
    # Aplicar exactamente la misma lógica que process_block pero sin paralelización
    df['garman_klass_vol'] = ((np.log(df['high'])-np.log(df['low']))**2)/2-(2*np.log(2)-1)*((np.log(df['close'])-np.log(df['open']))**2)

    df['rsi'] = df.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.rsi(close=x, length=20))

    df['bb_low'] = df.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,0])

    df['bb_mid'] = df.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,1])

    df['bb_high'] = df.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,2])

    def compute_atr(stock_data):
        atr = pandas_ta.atr(high=stock_data['high'],
                            low=stock_data['low'],
                            close=stock_data['close'],
                            length=14)
        return atr.sub(atr.mean()).div(atr.std())

    df['atr'] = df.groupby(level=1, group_keys=False).apply(compute_atr)

    def compute_macd(close):
        macd = pandas_ta.macd(close=close, length=20).iloc[:,0]
        return macd.sub(macd.mean()).div(macd.std())

    df['macd'] = df.groupby(level=1, group_keys=False)['adj close'].apply(compute_macd)

    df['dollar_volume'] = (df['adj close']*df['volume'])/1e6

    # Aplicar la parte final (idéntica a la versión paralela)
    last_cols = [c for c in df.columns.unique(0) if c not in ['dollar_volume', 'volume', 'open',
                                                          'high', 'low', 'close']]

    data = (
        pd.concat([
            df.unstack('ticker')['dollar_volume'].resample('M').mean().stack('ticker').to_frame('dollar_volume'),
            df.unstack()[last_cols].resample('M').last().stack('ticker')
            ],axis=1)
        ).dropna()

    return data


    
@ray.remote
def process_block(df_block):
    """
    Procesa un bloque del DataFrame con la lógica original de calculateIndicators
    """
    # Aplicar exactamente la misma lógica que la función original
    df_block['garman_klass_vol'] = ((np.log(df_block['high'])-np.log(df_block['low']))**2)/2-(2*np.log(2)-1)*((np.log(df_block['close'])-np.log(df_block['open']))**2)

    df_block['rsi'] = df_block.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.rsi(close=x, length=20))

    df_block['bb_low'] = df_block.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,0])

    df_block['bb_mid'] = df_block.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,1])

    df_block['bb_high'] = df_block.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:,2])

    def compute_atr(stock_data):
        atr = pandas_ta.atr(high=stock_data['high'],
                            low=stock_data['low'],
                            close=stock_data['close'],
                            length=14)
        return atr.sub(atr.mean()).div(atr.std())

    df_block['atr'] = df_block.groupby(level=1, group_keys=False).apply(compute_atr)

    def compute_macd(close):
        macd = pandas_ta.macd(close=close, length=20).iloc[:,0]
        return macd.sub(macd.mean()).div(macd.std())

    df_block['macd'] = df_block.groupby(level=1, group_keys=False)['adj close'].apply(compute_macd)

    df_block['dollar_volume'] = (df_block['adj close']*df_block['volume'])/1e6

    return df_block




def calculateIndicators(df, num_blocks=None):
    """
    Versión paralelizada de calculateIndicators que mantiene la lógica original
    
    Args:
        df: DataFrame original con MultiIndex (date, ticker)
        num_blocks: Número de bloques para paralelizar (default: número de núcleos CPU)
    
    Returns:
        DataFrame procesado exactamente igual que la función original
    """
    # Determinar número de bloques si no se especifica
    if num_blocks is None:
        num_blocks = ray.cluster_resources().get('CPU', 4)
        num_blocks = int(num_blocks)
    
    # Obtener todos los tickers únicos
    tickers = df.index.get_level_values(1).unique()
    
    # Dividir tickers en bloques
    tickers_per_block = len(tickers) // num_blocks
    if tickers_per_block == 0:
        tickers_per_block = 1
        num_blocks = len(tickers)
    
    # Crear bloques de tickers
    ticker_blocks = []
    for i in range(num_blocks):
        start_idx = i * tickers_per_block
        if i == num_blocks - 1:  # Último bloque toma todos los tickers restantes
            end_idx = len(tickers)
        else:
            end_idx = (i + 1) * tickers_per_block
        
        block_tickers = tickers[start_idx:end_idx]
        ticker_blocks.append(block_tickers)
    
    # Crear bloques de datos
    data_blocks = []
    for block_tickers in ticker_blocks:
        # Filtrar el DataFrame para este bloque de tickers
        block_mask = df.index.get_level_values(1).isin(block_tickers)
        df_block = df[block_mask].copy()
        data_blocks.append(df_block)
    
    # Procesar bloques en paralelo
    futures = [process_block.remote(block) for block in data_blocks]
    processed_blocks = ray.get(futures)
    
    # Concatenar todos los bloques procesados
    df = pd.concat(processed_blocks, axis=0).sort_index()
    
    # Aplicar la parte final de la función original (sin modificar)
    last_cols = [c for c in df.columns.unique(0) if c not in ['dollar_volume', 'volume', 'open',
                                                          'high', 'low', 'close']]

    data = (
        pd.concat([
            df.unstack('ticker')['dollar_volume'].resample('M').mean().stack('ticker').to_frame('dollar_volume'),
            df.unstack()[last_cols].resample('M').last().stack('ticker')
            ],axis=1)
        ).dropna()

    return data




#inicializate the app

if __name__ == '__main__':
    ray.init()
    app.run(host='0.0.0.0', port=5001, debug=False)  
    ray.shutdown()