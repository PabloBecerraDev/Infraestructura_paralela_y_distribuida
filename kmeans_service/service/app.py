import ray 
import pandas as pd 
import numpy as np
import pandas_ta
import pandas_datareader.data as web
from statsmodels.regression.rolling import RollingOLS
from sklearn.cluster import KMeans
import statsmodels.api as sm
import requests 
from flask import Flask, jsonify, Response


app = Flask(__name__)





@app.route('/Kmeans-getData', methods=["POST"])
def getData():
    try:
        # Llamada al microservicio 2
        response = requests.post("http://localhost:5001/getData")
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

        #data.info()



        # Valores objetivo del RSI para inicializar los centroides
        target_rsi_values = [30, 45, 55, 70]

        # Creamos una matriz de ceros con 4 filas (uno por cada valor de RSI) y 18 columnas (features)
        initial_centroids = np.zeros((len(target_rsi_values), 18))

        # Insertamos los valores de RSI en la columna 6 (índice base 0)
        initial_centroids[:, 6] = target_rsi_values


        def get_clusters(df):
            df['cluster'] = KMeans(
                n_clusters=4,
                random_state=0,
                init=initial_centroids
            ).fit(df).labels_
            return df


        data = data.dropna().groupby('date', group_keys=False).apply(get_clusters)

        print(data)

 
        json_data = data.reset_index().to_json(orient="records")
        return Response(data, mimetype='application/json')

    
    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500




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
    app.run(host='0.0.0.0', port=5002, debug=False)
    ray.shutdown()