import ray
from flask import Flask, jsonify, Response
import requests
import pandas as pd



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

        print(data)


        json_data = data.reset_index().to_json(orient="records")
        return Response(data, mimetype='application/json')

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500








if __name__ == '__main__':
    ray.init()
    app.run(host='0.0.0.0', port=5003, debug=False)  
    ray.shutdown()