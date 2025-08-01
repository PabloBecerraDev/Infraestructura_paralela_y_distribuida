import ray
from flask import Flask, jsonify, Response
import yfinance as yf
import requests
import pandas as pd
import numpy as np
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns
import datetime as dt  #agregar esto
import matplotlib.ticker as mtick # y esto tambien
import matplotlib.pyplot as plt
import time
import io
import base64
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

@app.route('/status', methods=["GET"])
def status():
    """Endpoint simple de status"""
    return jsonify({
        "status": "OK",
        "service": "Portfolio Optimization Service",
        "version": "1.0.0",
        "ray_initialized": ray.is_initialized()
    })


@app.route('/getResult', methods=["POST"])
def getResultService():
    """Endpoint que procesa los datos usando paralelización con Ray"""
    try:
        response = requests.post("http://servicio_kmeans:5002/Kmeans-getData")
        response.raise_for_status()

        data = pd.DataFrame(response.json())
        data.set_index(['date', 'ticker'], inplace=True)

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Usar versión paralela
        portafolio_df = processPortfolioParallel(data)

        # Descargar datos
        spy = yf.download(tickers='SPY', start='2015-01-01', end=dt.date.today())

        # Verificar si el DataFrame está vacío
        if spy.empty:
            raise ValueError("No se pudieron descargar datos de SPY")

        # Verificar qué columna usar (puede ser 'Adj Close' o ('Adj Close', 'SPY'))
        if 'Adj Close' in spy.columns:
            spy_close = spy[['Adj Close']]  # ← Doble corchete para mantener DataFrame
        elif ('Adj Close', 'SPY') in spy.columns:  # MultiIndex
            spy_close = spy[[('Adj Close', 'SPY')]]  # ← Doble corchete
        else:
            # Usar la columna Close si Adj Close no existe
            spy_close = spy[['Close']]  # ← Doble corchete

        # Calcular retornos
        spy_ret = np.log(spy_close).diff().dropna()
        spy_ret.columns = ['SPY Buy&Hold']

        # Merge con portfolio_df
        portafolio_df = portafolio_df.merge(spy_ret, left_index=True, right_index=True)


        # GENERAR GRÁFICO
        def generate_portfolio_chart(portfolio_df):
            """Genera el gráfico del portafolio y lo retorna como base64"""
            plt.style.use('ggplot')
            
            # Calcular retornos acumulativos
            portfolio_cumulative_return = np.exp(np.log1p(portfolio_df).cumsum()) - 1
            
            # Crear el gráfico
            fig, ax = plt.subplots(figsize=(16, 6))
            
            # Filtrar datos hasta 2023-09-29 si existen
            try:
                portfolio_cumulative_return[:'2023-09-29'].plot(ax=ax)
            except:
                # Si no hay datos hasta esa fecha, graficar todo
                portfolio_cumulative_return.plot(ax=ax)
            
            plt.title('Unsupervised Learning Trading Strategy Returns Over Time')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))
            plt.ylabel('Return')
            
            # Convertir a base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            
            # Codificar en base64
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close(fig)  # Liberar memoria
            
            return image_base64

        # Generar gráfico
        chart_base64 = generate_portfolio_chart(portafolio_df)

        print(portafolio_df)

        # Preparar respuesta con datos y gráfico
        portfolio_json = portafolio_df.reset_index().to_dict(orient="records")
        
        response_data = {
            "portfolio_data": portfolio_json,
            "chart_image": chart_base64,
            "chart_url": f"data:image/png;base64,{chart_base64}"
        }

        return jsonify(response_data)

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500


@app.route('/getResultSequential', methods=["POST"])
def getResultSequential():
    """Endpoint que procesa los datos de manera secuencial"""
    try:
        response = requests.post("http://servicio_kmeans:5002/Kmeans-getDataSequential")
        response.raise_for_status()

        data = pd.DataFrame(response.json())
        data.set_index(['date', 'ticker'], inplace=True)

        if not pd.api.types.is_datetime64_any_dtype(data.index.get_level_values(0)):
            data = data.copy()
            data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in data.index],
                names=['date', 'ticker']
            )

        # Usar versión secuencial
        result = processPortfolioSequential(data)

        print(result)

        json_data = result.reset_index().to_json(orient="records")
        return Response(json_data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500


@app.route('/comparePerformance', methods=["POST"])
def comparePerformance():
    """Endpoint que compara el tiempo de procesamiento entre versión paralela y secuencial"""
    try:
        # Obtener datos una sola vez para ambas comparaciones
        response = requests.post("http://servicio_kmeans:5002/Kmeans-getData")
        response.raise_for_status()

        # Preparar datos base
        base_data = pd.DataFrame(response.json())
        base_data.set_index(['date', 'ticker'], inplace=True)

        if not pd.api.types.is_datetime64_any_dtype(base_data.index.get_level_values(0)):
            base_data = base_data.copy()
            base_data.index = pd.MultiIndex.from_tuples(
                [(pd.to_datetime(ts, unit='ms'), ticker) for ts, ticker in base_data.index],
                names=['date', 'ticker']
            )

        # Hacer copias para cada test
        data_parallel = base_data.copy()
        data_sequential = base_data.copy()

        # Medir tiempo de procesamiento paralelo
        start_time_parallel = time.time()
        result_parallel = processPortfolioParallel(data_parallel)
        end_time_parallel = time.time()
        parallel_time = end_time_parallel - start_time_parallel

        # Medir tiempo de procesamiento secuencial
        start_time_sequential = time.time()
        result_sequential = processPortfolioSequential(data_sequential)
        end_time_sequential = time.time()
        sequential_time = end_time_sequential - start_time_sequential

        # Calcular mejora de rendimiento
        speedup = sequential_time / parallel_time if parallel_time > 0 else 0
        improvement_percentage = ((sequential_time - parallel_time) / sequential_time * 100) if sequential_time > 0 else 0

        return jsonify({
            "performance_comparison": {
                "parallel_processing": {
                    "time_seconds": round(parallel_time, 4),
                    "method": "Ray parallel portfolio optimization"
                },
                "sequential_processing": {
                    "time_seconds": round(sequential_time, 4),
                    "method": "Sequential portfolio optimization"
                },
                "performance_metrics": {
                    "speedup_factor": round(speedup, 2),
                    "improvement_percentage": round(improvement_percentage, 2),
                    "portfolio_returns_generated": len(result_parallel),
                    "unique_dates_processed": len(base_data.index.get_level_values(0).unique()),
                    "unique_tickers": len(base_data.index.get_level_values(1).unique())
                }
            }
        })

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


# Funciones Ray remotas para paralelización
@ray.remote
def optimize_portfolio_for_date(start_date, tickers, adj_close_df, returns_dataframe):
    """Optimiza el portafolio para una fecha específica en paralelo"""
    try:
        end_date = (pd.to_datetime(start_date) + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
        optimization_start_date = (pd.to_datetime(start_date) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')
        optimization_end_date = (pd.to_datetime(start_date) - pd.DateOffset(days=1)).strftime('%Y-%m-%d')

        # Verificar que las columnas existen en adj_close_df
        available_cols = [col for col in tickers if col in adj_close_df.columns]
        if not available_cols:
            return pd.DataFrame()

        optimization_df = adj_close_df[optimization_start_date:optimization_end_date][available_cols]
        
        if optimization_df.empty:
            return pd.DataFrame()

        # Intentar optimización Max Sharpe
        success = False
        try:
            weights = optimize_weights_remote(
                prices=optimization_df,
                lower_bound=round(1/(len(optimization_df.columns)*2), 3)
            )
            weights = pd.DataFrame([weights], columns=optimization_df.columns, index=[0])
            success = True
        except Exception:
            pass

        # Si falla, usar pesos iguales
        if not success:
            weights = pd.DataFrame(
                [[1/len(optimization_df.columns) for i in range(len(optimization_df.columns))]],
                columns=optimization_df.columns,
                index=[0]
            )

        temp_df = returns_dataframe[start_date:end_date]
        temp_df = temp_df[available_cols]
        
        if temp_df.empty:
            return pd.DataFrame()

        # Calcular retornos ponderados
        weights_expanded = pd.DataFrame(
            np.tile(weights.values, (len(temp_df), 1)),
            columns=weights.columns,
            index=temp_df.index
        )

        weighted_returns = temp_df * weights_expanded
        portfolio_returns = weighted_returns.sum(axis=1).to_frame('Strategy Return')

        return portfolio_returns

    except Exception as e:
        print(f"Error processing {start_date}: {e}")
        return pd.DataFrame()


def optimize_weights_remote(prices, lower_bound=0):
    """Función auxiliar para optimización de pesos (usada en Ray remote)"""
    returns = expected_returns.mean_historical_return(prices=prices, frequency=252)
    cov = risk_models.sample_cov(prices=prices, frequency=252)
    
    ef = EfficientFrontier(
        expected_returns=returns,
        cov_matrix=cov,
        weight_bounds=(lower_bound, .1),
        solver='SCS'
    )
    
    weights = ef.max_sharpe()
    return ef.clean_weights()


def processPortfolioParallel(data):
    """
    Versión paralela del procesamiento de portafolios usando Ray
    """
    # Obtener fechas filtradas
    fixed_dates = getFiltered(data)
    
    # Obtener datos de precios
    new_df = getNewDf(data)
    
    # Preparar datos para optimización paralela
    if isinstance(new_df.columns, pd.MultiIndex):
        adj_close_df = new_df['Adj Close']
    else:
        if 'Adj Close' in new_df.columns:
            adj_close_df = new_df[['Adj Close']]
        else:
            return pd.DataFrame()
    
    returns_dataframe = np.log(adj_close_df).diff()
    
    # Preparar trabajos paralelos
    optimization_futures = []
    
    for start_date, tickers in fixed_dates.items():
        optimization_futures.append(
            optimize_portfolio_for_date.remote(
                start_date, tickers, adj_close_df, returns_dataframe
            )
        )
    
    # Ejecutar optimizaciones en paralelo
    portfolio_results = ray.get(optimization_futures)
    
    # Combinar resultados
    valid_results = [result for result in portfolio_results if not result.empty]
    
    if valid_results:
        portfolio_df = pd.concat(valid_results, axis=0)
        portfolio_df = portfolio_df.drop_duplicates()
        return portfolio_df
    else:
        return pd.DataFrame()


def processPortfolioSequential(data):
    """
    Versión secuencial del procesamiento de portafolios (lógica original)
    """
    fixed_dates = getFiltered(data)
    new_df = getNewDf(data)
    portafolio = getPortafolio(new_df, fixed_dates)
    return portafolio


def getFiltered(data):
    """Función original sin cambios"""
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
    """Función original sin cambios"""
    returns = expected_returns.mean_historical_return(prices=prices, frequency=252)
    cov = risk_models.sample_cov(prices=prices, frequency=252)
    
    ef = EfficientFrontier(
        expected_returns=returns,
        cov_matrix=cov,
        weight_bounds=(lower_bound, .1),
        solver='SCS'
    )
    
    weights = ef.max_sharpe()
    return ef.clean_weights()


def getNewDf(data):
    """Función original sin cambios"""
    stocks = data.index.get_level_values('ticker').unique().tolist()

    new_df = yf.download(
        tickers=stocks,
        start=data.index.get_level_values('date').unique()[0]-pd.DateOffset(months=12),
        end=data.index.get_level_values('date').unique()[-1],
        auto_adjust=False
    )

    return new_df


def getPortafolio(new_df, fixed_dates):
    """Función original sin cambios"""
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
    print("Portfolio Optimization Service starting on:")
    print("- http://localhost:5003")
    print("- http://127.0.0.1:5003")
    print("- http://0.0.0.0:5003")
    app.run(host='0.0.0.0', port=5003, debug=False)  
    ray.shutdown()