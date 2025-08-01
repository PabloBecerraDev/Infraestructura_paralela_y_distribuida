from flask import Flask, jsonify, Response
import pandas as pd
import yfinance as yf
import ray
import math
from flask_cors import CORS

# Inicializar Ray
ray.init(ignore_reinit_error=True)

app = Flask(__name__)
CORS(app)

# Configuración para manejar respuestas grandes
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB
app.config['JSON_SORT_KEYS'] = False
   
@ray.remote
def descargar_chunk_datos(tickers_chunk, start_date, end_date):
    """
    Función remota de Ray para descargar datos de un chunk de tickers
    """
    try:
        print(f"Procesando chunk con {len(tickers_chunk)} símbolos: {tickers_chunk[:3]}...")
        
        df = yf.download(
            tickers=tickers_chunk,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False
        )
        
        print(f"Descarga completada. Shape: {df.shape}")
        
        # Verificar si obtuvimos datos
        if df.empty:
            print(f"DataFrame vacío para chunk: {tickers_chunk}")
            return pd.DataFrame()
        
        # Stack exactamente como en el código original
        df_stacked = df.stack()
        df_stacked.index.names = ['date', 'ticker']
        df_stacked.columns = df_stacked.columns.str.lower()
        
        result = df_stacked.reset_index()
        print(f"Chunk procesado. Resultado shape: {result.shape}")
        print(f"Primeras filas del chunk:\n{result.head(2)}")
        
        return result
        
    except Exception as e:
        print(f"ERROR en chunk {tickers_chunk}: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


@app.route("/sp500_data", methods=["POST"])
def get_sp500_data():
    """
    Endpoint paralelizado para obtener datos del S&P 500
    """
    try:
        # Obtener listado del S&P 500
        print("Obteniendo lista de símbolos del S&P 500...")

        # esto es un dataframe, es la primer tabla de la pagina
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0] 
        print("Tabla de Wikipedia descargada:")
        print(sp500.head())
        sp500['Symbol'] = sp500['Symbol'].str.replace('.', '-') #en toda la columna de Symbol reemplaza . por - 
        #symbols_list = sp500['Symbol'].unique().tolist() #es una lista nica de los simbolos 
        symbols_list = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
        print(f"Símbolos obtenidos: {symbols_list[:5]}")

        # this loop remove the repeated symbol of the symbol_list
        symbols_to_remove = ['SW', 'SOLV', 'VLTO', 'GEV']
        for symbol in symbols_to_remove:
            if symbol in symbols_list:
                symbols_list.remove(symbol)

        print(f"Total de símbolos a procesar: {len(symbols_list)}")

        # Configurar fechas
        end_date = '2023-09-27'
        start_date = pd.to_datetime(end_date) - pd.DateOffset(365 * 8) # this is teh date from 8 years ago
        
        print(f"Rango de fechas: {start_date.date()} a {end_date}")

        # Para debugging: Primero probemos con menos chunks
        num_chunks = 4  
        # divide the legth of tehe symbol _list by the num_chunks then apply the ceil function
        chunk_size = math.ceil(len(symbols_list) / num_chunks)
        
        # Crear chunks de símbolos
        symbol_chunks = [] # -->  [['AAPL', 'MSFT', ...], ['GOOG', 'AMZN', ...], ...]
        for i in range(0, len(symbols_list), chunk_size):
            chunk = symbols_list[i:i + chunk_size]
            symbol_chunks.append(chunk)

        print(f"Dividiendo trabajo en {len(symbol_chunks)} chunks")
        for i, chunk in enumerate(symbol_chunks):
            print(f"Chunk {i+1}: {len(chunk)} símbolos - {chunk[:3]}...")

        # Lanzar tareas paralelas con Ray
        print("Iniciando descarga paralela...")
        futures = []
        for i, chunk in enumerate(symbol_chunks):
            future = descargar_chunk_datos.remote(chunk, start_date, end_date)
            futures.append(future)

        # Esperar y recolectar resultados
        print("Esperando resultados de Ray...")
        dataframes = ray.get(futures)
        
        print(f"Recibidos {len(dataframes)} DataFrames de Ray")
        
        # Debug: verificar cada DataFrame
        valid_dataframes = []
        for i, df in enumerate(dataframes):
            if not df.empty:
                print(f"DataFrame {i}: Shape {df.shape}, Columns: {df.columns.tolist()}")
                valid_dataframes.append(df)
            else:
                print(f"DataFrame {i}: VACÍO")
        
        if not valid_dataframes:
            return jsonify({"error": "No se pudieron obtener datos válidos"}), 500

        print(f"DataFrames válidos: {len(valid_dataframes)}")

        # Concatenar resultados
        print("Concatenando DataFrames...")
        df_final = pd.concat(valid_dataframes, ignore_index=True)
        
        print(f"DataFrame final concatenado:")
        print(f"  - Shape: {df_final.shape}")
        print(f"  - Columnas: {df_final.columns.tolist()}")
        print(f"  - Tipos de datos: {df_final.dtypes.to_dict()}")
        print(f"  - Memoria usada: {df_final.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Verificar sample de datos
        print("Muestra de datos:")
        print(df_final.head(3))
        
        # Verificar que tenemos datos
        if df_final.empty:
            return jsonify({"error": "DataFrame final vacío"}), 500

        # Convertir a JSON con verificación de tamaño
        print("Convirtiendo a JSON...")
        try:
            df_json = df_final.to_json(orient="records")
            json_size_mb = len(df_json.encode('utf-8')) / 1024 / 1024
            print(f"JSON generado. Tamaño: {json_size_mb:.2f} MB")
            
            if json_size_mb > 100:  # Warn si es muy grande
                print(f"ADVERTENCIA: JSON muy grande ({json_size_mb:.2f} MB)")
            
        except Exception as e:
            print(f"ERROR al convertir a JSON: {str(e)}")
            return jsonify({"error": f"Error al convertir a JSON: {str(e)}"}), 500

        print("Enviando respuesta...")
        response = Response(df_json, mimetype='application/json')
        response.headers['Content-Length'] = len(df_json.encode('utf-8'))
        response.headers['Cache-Control'] = 'no-cache'
        return response

    except Exception as e:
        print(f"Error en get_sp500_data: {str(e)}")
        return jsonify({"error": f"Error interno: {str(e)}"}), 500




@app.route("/sp500_data_original", methods=["POST"])
def get_sp500_data_original():
    """
    Versión original sin paralelizar para comparar
    """
    try:
        print("=== VERSIÓN ORIGINAL (SIN RAY) ===")
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        sp500['Symbol'] = sp500['Symbol'].str.replace('.', '-')

        symbols_list = sp500['Symbol'].unique().tolist()

        # Quitar símbolos que dan error en yfinance
        for i in ['SW', 'SOLV', 'VLTO', 'GEV']:
            if i in symbols_list:
                symbols_list.remove(i)

        print(f"Símbolos a procesar: {len(symbols_list)}")

        end_date = '2023-09-27'
        start_date = pd.to_datetime(end_date) - pd.DateOffset(365 * 8)

        print("Descargando datos (versión original)...")
        df = yf.download(
            tickers=symbols_list,
            start=start_date,
            end=end_date,
            auto_adjust=False
        ).stack()

        df.index.names = ['date', 'ticker']
        df.columns = df.columns.str.lower()

        df_reset = df.reset_index()
        
        print(f"DataFrame original:")
        print(f"  - Shape: {df_reset.shape}")
        print(f"  - Columnas: {df_reset.columns.tolist()}")
        print(f"  - Memoria: {df_reset.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Convertir a JSON
        df_json = df_reset.to_json(orient="records")
        json_size = len(df_json.encode('utf-8')) / 1024 / 1024
        print(f"JSON original size: {json_size:.2f} MB")

        response = Response(df_json, mimetype='application/json')
        response.headers['Content-Length'] = len(df_json.encode('utf-8'))
        return response
        
    except Exception as e:
        print(f"Error en versión original: {str(e)}")
        return jsonify({"error": f"Error: {str(e)}"}), 500



@app.route("/compare", methods=["POST"])
def compare_methods():
    """
    Endpoint para comparar ambos métodos sin enviar todos los datos
    """
    try:
        print("=== COMPARANDO MÉTODOS ===")
        
        # Datos básicos
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        sp500['Symbol'] = sp500['Symbol'].str.replace('.', '-')
        symbols_list = sp500['Symbol'].unique().tolist()
        
        for i in ['SW', 'SOLV', 'VLTO', 'GEV']:
            if i in symbols_list:
                symbols_list.remove(i)

        end_date = '2023-09-27'
        start_date = pd.to_datetime(end_date) - pd.DateOffset(365 * 8)
        
        # Tomar solo una pequeña muestra para comparar
        sample_symbols = symbols_list[:10]  # Solo 10 símbolos
        
        print(f"Comparando con muestra de {len(sample_symbols)} símbolos")
        
        # Método original
        print("1. Método ORIGINAL...")
        df_original = yf.download(
            tickers=sample_symbols,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False
        ).stack()
        df_original.index.names = ['date', 'ticker']
        df_original.columns = df_original.columns.str.lower()
        df_original_reset = df_original.reset_index()
        
        # Método paralelizado
        print("2. Método PARALELIZADO...")
        future = descargar_chunk_datos.remote(sample_symbols, start_date, end_date)
        df_parallel = ray.get(future)
        
        # Comparar
        comparison = {
            "original_shape": df_original_reset.shape,
            "parallel_shape": df_parallel.shape,
            "original_columns": df_original_reset.columns.tolist(),
            "parallel_columns": df_parallel.columns.tolist(),
            "shapes_match": df_original_reset.shape == df_parallel.shape,
            "columns_match": df_original_reset.columns.tolist() == df_parallel.columns.tolist(),
            "sample_symbols": sample_symbols
        }
        
        if comparison["shapes_match"] and comparison["columns_match"]:
            # Comparar algunas filas
            comparison["first_rows_match"] = df_original_reset.head().equals(df_parallel.head())
        
        return jsonify(comparison)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status", methods=["GET"])
def ray_status():
    """
    Endpoint para verificar el estado de Ray
    """
    try:
        cluster_resources = ray.cluster_resources()
        return jsonify({
            "ray_initialized": ray.is_initialized(),
            "cluster_resources": cluster_resources,
            "available_resources": ray.available_resources()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    try:
        print("Iniciando microservicio S&P 500...")
        print(f"Ray inicializado: {ray.is_initialized()}")
        app.run(host='0.0.0.0', port=5000, debug=False)  # debug=False en producción0
    finally:
        # Limpiar Ray al cerrar la aplicación
        if ray.is_initialized():
            ray.shutdown()




            