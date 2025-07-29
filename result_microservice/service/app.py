import ray
from flask import Flask, jsonify, Response
import yfinance as yf
import requests
import pandas as pd
import numpy as np
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

        #print(fixed_dates)




        new_df = getNewDf(data)
        #print(new_df)

        portafolio = getPortafolio(new_df, fixed_dates)

        print(portafolio)


        json_data = data.reset_index().to_json(orient="records")
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500






def getFiltered(data):
    filtered_df = data[data['cluster']==3].copy()

    filtered_df = filtered_df.reset_index(level=1)

    filtered_df.index = filtered_df.index+pd.DateOffset(1)

    filtered_df = filtered_df.reset_index().set_index(['date', 'ticker'])

    dates = filtered_df.index.get_level_values('date').unique().tolist()

    fixed_dates = {}

    for d in dates:

        fixed_dates[d.strftime('%Y-%m-%d')] = filtered_df.xs(d, level=0).index.tolist()

    return fixed_dates



def optimize_weights(prices, lower_bound=0):

    returns = expected_returns.mean_historical_return(prices=prices,
                                                      frequency=252)

    cov = risk_models.sample_cov(prices=prices,
                                 frequency=252)

    ef = EfficientFrontier(expected_returns=returns,
                           cov_matrix=cov,
                           weight_bounds=(lower_bound, .1),
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




def getPortafolio(new_df, fixed_dates):
    print("=== DEBUG INFO ===")
    print(f"new_df shape: {new_df.shape}")
    print(f"new_df columns: {new_df.columns}")
    print(f"new_df columns type: {type(new_df.columns)}")
    print(f"First few rows of new_df:")
    print(new_df.head())
    print("==================")
    
    # Verificar si tiene MultiIndex en columnas o no
    if isinstance(new_df.columns, pd.MultiIndex):
        # Caso normal: múltiples tickers
        adj_close_df = new_df['Adj Close']
        print("Using MultiIndex columns")
    else:
        # Caso de un solo ticker o estructura diferente
        if 'Adj Close' in new_df.columns:
            adj_close_df = new_df[['Adj Close']]
            print("Using single column structure")
        else:
            print("ERROR: No 'Adj Close' column found")
            print(f"Available columns: {new_df.columns.tolist()}")
            return pd.DataFrame()
    
    print(f"adj_close_df shape: {adj_close_df.shape}")
    print(f"adj_close_df columns: {adj_close_df.columns}")
    
    returns_dataframe = np.log(adj_close_df).diff()
    portfolio_df = pd.DataFrame()

    for start_date in fixed_dates.keys():
        try:
            end_date = (pd.to_datetime(start_date)+pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
            cols = fixed_dates[start_date]
            optimization_start_date = (pd.to_datetime(start_date)-pd.DateOffset(months=12)).strftime('%Y-%m-%d')
            optimization_end_date = (pd.to_datetime(start_date)-pd.DateOffset(days=1)).strftime('%Y-%m-%d')

            print(f"Processing {start_date}, cols: {cols}")
            
            # Verificar que las columnas existen en adj_close_df
            available_cols = [col for col in cols if col in adj_close_df.columns]
            if not available_cols:
                print(f"No matching columns found for {start_date}")
                continue
                
            print(f"Available cols: {available_cols}")
            
            optimization_df = adj_close_df[optimization_start_date:optimization_end_date][available_cols]
            
            if optimization_df.empty:
                print(f"Empty optimization_df for {start_date}")
                continue

            success = False
            try:
                weights = optimize_weights(prices=optimization_df,
                                    lower_bound=round(1/(len(optimization_df.columns)*2),3))

                weights = pd.DataFrame([weights], columns=optimization_df.columns, index=[0])
                success = True
            except Exception as opt_error:
                print(f'Max Sharpe Optimization failed for {start_date}: {opt_error}, Continuing with Equal-Weights')

            if success==False:
                weights = pd.DataFrame([[1/len(optimization_df.columns) for i in range(len(optimization_df.columns))]],
                                    columns=optimization_df.columns,
                                    index=[0])

            temp_df = returns_dataframe[start_date:end_date]

            # Filtrar solo las columnas que están en weights
            temp_df = temp_df[available_cols]
            
            if temp_df.empty:
                print(f"Empty temp_df for {start_date}")
                continue

            # Enfoque más simple: multiplicar directamente
            weights_expanded = pd.DataFrame(
                np.tile(weights.values, (len(temp_df), 1)),
                columns=weights.columns,
                index=temp_df.index
            )

            # Calcular retornos ponderados directamente
            weighted_returns = temp_df * weights_expanded

            # Sumar por filas para obtener el retorno del portfolio
            portfolio_returns = weighted_returns.sum(axis=1).to_frame('Strategy Return')

            portfolio_df = pd.concat([portfolio_df, portfolio_returns], axis=0)

        except Exception as e:
            print(f"Error processing {start_date}: {e}")
            import traceback
            traceback.print_exc()

    portfolio_df = portfolio_df.drop_duplicates()
    return portfolio_df







if __name__ == '__main__':
    ray.init()
    app.run(host='0.0.0.0', port=5003, debug=False)  
    ray.shutdown()