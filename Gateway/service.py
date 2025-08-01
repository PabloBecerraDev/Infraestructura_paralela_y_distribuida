from flask import Flask, request, Response, jsonify
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

MICROSERVICES = {
    "process": "http://servicio_process_data:5001/getData",
    "market": "http://servicio_get_data:5000/sp500_data",
    "cluster": "http://servicio_kmeans:5002/Kmeans-getData",
    "portfolio": "http://servicio_get_result:5003/getResult"
}

@app.route("/api/<service>", methods=["POST", "GET"])
def route_to_service(service):
    if service not in MICROSERVICES:
        return jsonify({"error": "Unknown service"}), 404

    url = MICROSERVICES[service]
    try:
        if request.method == "POST":
            upstream_response = requests.post(url, json=request.get_json(silent=True) or {})
        else:
            upstream_response = requests.get(url)

        # Intenta retornar como JSON si es posible
        try:
            data = upstream_response.json()
            return jsonify(data), upstream_response.status_code
        except ValueError:
            # Si no es JSON, devolver como respuesta cruda
            return Response(
                upstream_response.content,
                status=upstream_response.status_code,
                content_type=upstream_response.headers.get('Content-Type', 'application/octet-stream')
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status")
def status():
    return jsonify({"gateway": "OK", "services": list(MICROSERVICES.keys())})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
