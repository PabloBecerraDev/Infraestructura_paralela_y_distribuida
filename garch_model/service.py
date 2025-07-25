from fastapi import FastAPI
from pydantic import BaseModel
import ray
from ray import serve
from arch import arch_model
import numpy as np
import time

app = FastAPI()

class InputData(BaseModel):
    returns: list

@serve.deployment
@serve.ingress(app)
class GARCHService:
    def __init__(self):
        pass

    @app.get("/health")
    async def health(self):
        return {"status": "garch model running"}

    @app.post("/predict")
    async def predict(self, data: InputData):
        returns = np.array(data.returns)
        if len(returns) < 10:
            return {"error": "Insufficient data points"}
        model = arch_model(returns, p=1, q=3)
        model_fitted = model.fit(disp="off")
        forecast = model_fitted.forecast(horizon=1)
        return {"variance_forecast": forecast.variance.values[-1][0]}

if __name__ == "__main__":
    ray.init()
    serve.start(detached=True, http_options={"host": "0.0.0.0", "port": 8001})
    serve.run(GARCHService.bind(), name="default")

    while True:
        time.sleep(3600)
