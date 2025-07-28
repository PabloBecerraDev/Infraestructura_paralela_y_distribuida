import ray
from flask import Flask, jsonify, Response
import yfinance as yf
import requests
import pandas as pd
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns



app = Flask(__name__)



@app.route('/getResult', methods=["POST"])
def getResultService():
    try:
        response = requests.post("http://localhost:5002/Kmeans-getData")
        response.raise_for_status()


        data = pd.DataFrame(response.json())

        data.set_index(['date', 'ticker'], inplace=True)


        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )


        fixed_dates = getFiltered(data)


        new_df = getNewDf(data)

        print(new_df)


        json_data = data.reset_index().to_json(orient="records")
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500






def getFiltered(df):
    filtered_df = df[df['cluster']==3].copy()

    filtered_df = filtered_df.reset_index(level=1)

    filtered_df.index = filtered_df.index+pd.DateOffset(1)

    filtered_df = filtered_df.reset_index().set_index(['date', 'ticker'])

    dates = filtered_df.index.get_level_values('date').unique().tolist()

    fixed_dates = {}

    for d in dates:

        fixed_dates[d.strftime('%Y-%m-%d')] = filtered_df.xs(d, level=0).index.tolist()
    
    return fixed_dates



def optimize_weights(prices, lower_bound=0):
    """
    Optimiza los pesos usando Maximum Sharpe Ratio
    """
    returns = expected_returns.mean_historical_return(prices=prices, frequency=252)
    cov = risk_models.sample_cov(prices=prices, frequency=252)
    
    ef = EfficientFrontier(expected_returns=returns,
                           cov_matrix=cov,
                           weight_bounds=(lower_bound, 0.1),
                           solver='SCS')
    
    weights = ef.max_sharpe()
    return ef.clean_weights()



def getNewDf(data):
    stocks = data.index.get_level_values('ticker').unique().tolist()

    new_df = yf.download(tickers=stocks,
                        start=data.index.get_level_values('date').unique()[0]-pd.DateOffset(months=12),
                        end=data.index.get_level_values('date').unique()[-1],
                        auto_adjust=False)

    return new_df



if __name__ == '__main__':
    ray.init()
    app.run(host='0.0.0.0', port=5003, debug=False)  
    ray.shutdown()