import ray 
import pandas as pd 
import numpy as np
import pandas_ta
import pandas_datareader.data as web
from statsmodels.regression.rolling import RollingOLS
from sklearn.cluster import KMeans
import statsmodels.api as sm
import requests 
import time
from flask import Flask, jsonify, Response
from flask_cors import CORS


app = Flask(__name__)
CORS(app)   

@app.route('/status', methods=["GET"])
def status():
    """Endpoint simple de status"""
    return jsonify({
        "status": "OK",
        "service": "K-means Financial Clustering Service",
        "version": "1.0.0",
        "ray_initialized": ray.is_initialized()
    })


@app.route('/Kmeans-getData', methods=["POST"])
def getData():
    """Endpoint que procesa los datos usando paralelización con Ray"""
    try:
        # Llamada al microservicio 2 (versión paralela)
        response = requests.post("http://servicio_process_data:5001/getData")
        response.raise_for_status()

        # Cargar el JSON como DataFrame
        data = pd.DataFrame(response.json())

        data.set_index(['date', 'ticker'], inplace=True)

        expected_cols = ['adj close', 'garman_klass_vol', 'rsi', 'bb_low', 'bb_mid', 'bb_high', 'atr', 'macd']
        data = data[expected_cols]

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Usar versión paralela del procesamiento
        data = processDataParallel(data)

        print(data)

        json_data = data.reset_index().to_json(orient="records")
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500


@app.route('/Kmeans-getDataSequential', methods=["POST"])
def getDataSequential():
    """Endpoint que procesa los datos de manera secuencial (no paralela)"""
    try:
        # Llamada al microservicio 2 (usando la versión secuencial)
        response = requests.post("http://servicio_process_data:5001/getDataSequential")
        response.raise_for_status()

        # Cargar el JSON como DataFrame
        data = pd.DataFrame(response.json())

        data.set_index(['date', 'ticker'], inplace=True)

        expected_cols = ['adj close', 'garman_klass_vol', 'rsi', 'bb_low', 'bb_mid', 'bb_high', 'atr', 'macd']
        data = data[expected_cols]

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Usar versión secuencial del procesamiento
        data = processDataSequential(data)

        print(data)

        json_data = data.reset_index().to_json(orient="records")
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500


@app.route('/comparePerformance', methods=["POST"])
def comparePerformance():
    """Endpoint que compara el tiempo de procesamiento entre versión paralela y secuencial"""
    try:
        # Obtener datos una sola vez para ambas comparaciones
        response = requests.post("http://servicio_process_data:5001/getData")
        response.raise_for_status()

        # Preparar datos base
        base_data = pd.DataFrame(response.json())
        base_data.set_index(['date', 'ticker'], inplace=True)

        expected_cols = ['adj close', 'garman_klass_vol', 'rsi', 'bb_low', 'bb_mid', 'bb_high', 'atr', 'macd']
        base_data = base_data[expected_cols]

        if not pd.api.types.is_datetime64_any_dtype(base_data.index.get_level_values(0)):
            base_data = base_data.copy()
            base_data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in base_data.index],
                names=['date', 'ticker']
            )

        # Hacer copias para cada test
        data_parallel = base_data.copy()
        data_sequential = base_data.copy()

        # Medir tiempo de procesamiento paralelo (nueva versión con Ray)
        start_time_parallel = time.time()
        result_parallel = processDataParallel(data_parallel)
        end_time_parallel = time.time()
        parallel_time = end_time_parallel - start_time_parallel

        # Medir tiempo de procesamiento secuencial (versión original)
        start_time_sequential = time.time()
        result_sequential = processDataSequential(data_sequential)
        end_time_sequential = time.time()
        sequential_time = end_time_sequential - start_time_sequential

        # Calcular mejora de rendimiento
        speedup = sequential_time / parallel_time if parallel_time > 0 else 0
        improvement_percentage = ((sequential_time - parallel_time) / sequential_time * 100) if sequential_time > 0 else 0

        return jsonify({
            "performance_comparison": {
                "parallel_processing": {
                    "time_seconds": round(parallel_time, 4),
                    "method": "Ray parallel processing + parallel clustering"
                },
                "sequential_processing": {
                    "time_seconds": round(sequential_time, 4),
                    "method": "Sequential processing"
                },
                "performance_metrics": {
                    "speedup_factor": round(speedup, 2),
                    "improvement_percentage": round(improvement_percentage, 2),
                    "data_rows_processed": len(result_parallel),
                    "unique_tickers": len(base_data.index.get_level_values(1).unique()),
                    "clusters_generated": len(result_parallel['cluster'].unique()) if 'cluster' in result_parallel.columns else 0
                }
            }
        })

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


# Funciones Ray remotas para paralelización
@ray.remote
def process_ticker_returns(ticker_data, ticker_name):
    """Calcula returns para un ticker específico"""
    ticker_df = ticker_data.copy()
    ticker_df.index = pd.MultiIndex.from_product([[ticker_name], ticker_df.index], names=['ticker', 'date'])
    ticker_df = ticker_df.swaplevel().sort_index()
    
    result = calculate_returns(ticker_df)
    return result

@ray.remote
def process_ticker_betas(ticker_data, ticker_name, factor_columns):
    """Calcula betas para un ticker específico usando RollingOLS"""
    if len(ticker_data) < 10:  # Mínimo de observaciones requeridas
        return pd.DataFrame()
    
    try:
        # Preparar datos para RollingOLS
        endog = ticker_data['return_1m'].dropna()
        exog_data = ticker_data[factor_columns].dropna()
        
        # Asegurar que tenemos datos alineados
        common_index = endog.index.intersection(exog_data.index)
        if len(common_index) < len(factor_columns) + 1:
            return pd.DataFrame()
            
        endog = endog.loc[common_index]
        exog_data = exog_data.loc[common_index]
        exog = sm.add_constant(exog_data)
        
        # Calcular rolling betas
        window_size = min(24, len(common_index))
        min_obs = len(factor_columns) + 1
        
        rolling_ols = RollingOLS(
            endog=endog,
            exog=exog,
            window=window_size,
            min_nobs=min_obs
        )
        
        results = rolling_ols.fit(params_only=True)
        betas = results.params.drop('const', axis=1)
        
        # Crear MultiIndex para el resultado
        betas.index = pd.MultiIndex.from_product([[ticker_name], betas.index], names=['ticker', 'date'])
        
        return betas
        
    except Exception as e:
        print(f"Error processing betas for {ticker_name}: {e}")
        return pd.DataFrame()

@ray.remote
def process_date_clustering(date_data, date_value, initial_centroids):
    """Procesa clustering para una fecha específica"""
    if len(date_data) == 0:
        return pd.DataFrame()
    
    try:
        date_df = date_data.copy()
        
        # Aplicar K-means clustering
        kmeans = KMeans(
            n_clusters=4,
            random_state=0,
            init=initial_centroids,
            n_init=1  # Solo una inicialización ya que proporcionamos centroides
        )
        
        date_df['cluster'] = kmeans.fit(date_df).labels_
        
        # Recrear el MultiIndex
        date_df.index = pd.MultiIndex.from_product(
            [[date_value], date_df.index], 
            names=['date', 'ticker']
        )
        
        return date_df
        
    except Exception as e:
        print(f"Error processing clustering for date {date_value}: {e}")
        return pd.DataFrame()


def processDataParallel(data):
    """
    Versión paralela del procesamiento usando Ray
    """
    # 1. Paralelizar cálculo de returns por ticker
    tickers = data.index.get_level_values(1).unique()
    
    # Preparar datos por ticker para returns
    ticker_data_list = []
    ticker_names = []
    
    for ticker in tickers:
        ticker_df = data.xs(ticker, level=1).copy()
        if len(ticker_df) > 0:
            ticker_data_list.append(ticker_df)
            ticker_names.append(ticker)
    
    # Procesar returns en paralelo
    return_futures = [
        process_ticker_returns.remote(ticker_data, ticker_name)
        for ticker_data, ticker_name in zip(ticker_data_list, ticker_names)
    ]
    
    return_results = ray.get(return_futures)
    
    # Combinar resultados de returns
    valid_results = [result for result in return_results if not result.empty]
    if not valid_results:
        return pd.DataFrame()
        
    data = pd.concat(valid_results, axis=0).sort_index().dropna()
    
    # 2. Generar factor data (esto es secuencial por naturaleza)
    factor_data = factorDataGen(data)
    
    # 3. Filtrar stocks válidos
    observations = factor_data.groupby(level=1).size()
    valid_stocks = observations[observations >= 10]
    factor_data = factor_data[factor_data.index.get_level_values('ticker').isin(valid_stocks.index)]
    
    # 4. Paralelizar cálculo de betas por ticker
    factor_columns = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
    valid_tickers = factor_data.index.get_level_values(1).unique()
    
    beta_futures = []
    for ticker in valid_tickers:
        ticker_factor_data = factor_data.xs(ticker, level=1)
        if len(ticker_factor_data) >= 10:
            beta_futures.append(
                process_ticker_betas.remote(ticker_factor_data, ticker, factor_columns)
            )
    
    beta_results = ray.get(beta_futures)
    
    # Combinar resultados de betas
    valid_beta_results = [result for result in beta_results if not result.empty]
    if valid_beta_results:
        betas = pd.concat(valid_beta_results, axis=0).sort_index()
    else:
        return pd.DataFrame()
    
    # 5. Unir datos con betas y preparar para clustering
    factors = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
    data = data.join(betas.groupby('ticker').shift())
    data.loc[:, factors] = data.groupby('ticker', group_keys=False)[factors].apply(lambda x: x.fillna(x.mean()))
    data = data.drop('adj close', axis=1)
    data = data.dropna()
    
    # 6. Paralelizar clustering por fecha
    target_rsi_values = [30, 45, 55, 70]
    initial_centroids = np.zeros((len(target_rsi_values), 18))
    initial_centroids[:, 6] = target_rsi_values
    
    dates = data.index.get_level_values(0).unique()
    
    clustering_futures = []
    for date in dates:
        try:
            date_data = data.xs(date, level=0)
            if len(date_data) > 0:
                clustering_futures.append(
                    process_date_clustering.remote(date_data, date, initial_centroids)
                )
        except KeyError:
            continue
    
    clustering_results = ray.get(clustering_futures)
    
    # Combinar resultados de clustering
    valid_clustering_results = [result for result in clustering_results if not result.empty]
    if valid_clustering_results:
        data = pd.concat(valid_clustering_results, axis=0).sort_index()
    else:
        return pd.DataFrame()
    
    return data


def processDataSequential(data):
    """
    Versión secuencial del procesamiento completo (lógica original)
    """
    data = data.groupby(level=1, group_keys=False).apply(calculate_returns).dropna()
    factor_data = factorDataGen(data)

    observations = factor_data.groupby(level=1).size()
    valid_stocks = observations[observations >= 10]
    factor_data = factor_data[factor_data.index.get_level_values('ticker').isin(valid_stocks.index)]

    betas = calculateBetas(factor_data)

    factors = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
    data = (data.join(betas.groupby('ticker').shift()))
    data.loc[:, factors] = data.groupby('ticker', group_keys=False)[factors].apply(lambda x: x.fillna(x.mean()))
    data = data.drop('adj close', axis=1)
    data = data.dropna()

    # Clustering secuencial
    target_rsi_values = [30, 45, 55, 70]
    initial_centroids = np.zeros((len(target_rsi_values), 18))
    initial_centroids[:, 6] = target_rsi_values

    def get_clusters(df):
        df['cluster'] = KMeans(
            n_clusters=4,
            random_state=0,
            init=initial_centroids
        ).fit(df).labels_
        return df

    data = data.dropna().groupby('date', group_keys=False).apply(get_clusters)
    
    return data


def calculate_returns(df):
  outlier_cutoff = 0.005

  lags = [1, 2, 3, 6, 9, 12]

  for lag in lags:
      df[f'return_{lag}m'] = (
          df['adj close']
          .pct_change(lag)
          .pipe(lambda x: x.clip(lower=x.quantile(outlier_cutoff), upper=x.quantile(1-outlier_cutoff)))
          .add(1)
          .pow(1/lag)
          .sub(1))

  return df


def factorDataGen(df):
    factor_data = web.DataReader('F-F_Research_Data_5_Factors_2x3',
                             'famafrench',
                             start='2010')[0].drop('RF', axis=1)

    factor_data.index = factor_data.index.to_timestamp()

    factor_data = factor_data.resample('M').last().div(100)

    factor_data.index.name = 'date'

    factor_data = factor_data.join(df['return_1m']).sort_index()

    return factor_data


def calculateBetas(df):
    betas = (df.groupby(level=1, group_keys=False)
         .apply(lambda x: RollingOLS(endog=x['return_1m'],
                                     exog=sm.add_constant(x.drop('return_1m', axis=1)),
                                     window=min(24, x.shape[0]),
                                     min_nobs=len(x.columns)+1)
         .fit(params_only=True)
         .params
         .drop('const', axis=1)))
    
    return betas


#inicializate the app 

if __name__ == '__main__':
    ray.init()
    print("K-means Clustering Service starting on:")
    print("- http://localhost:5002")
    print("- http://127.0.0.1:5002")
    print("- http://0.0.0.0:5002")
    app.run(host='0.0.0.0', port=5002, debug=False)
    ray.shutdown()