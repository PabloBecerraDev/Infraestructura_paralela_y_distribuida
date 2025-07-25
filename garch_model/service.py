import ray
from ray import serve
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
from arch import arch_model

# Inicializa Ray y Ray Serve
ray.init()
app = FastAPI()
serve.start(detached=True)

class ReturnInput(BaseModel):
    returns: List[float]

@serve.deployment
class GARCHService:
    def __init__(self):
        pass

    async def __call__(self, request):
        body = await request.json()
        returns = pd.Series(body["returns"])
        model = arch_model(returns, p=1, q=3)
        res = model.fit(update_freq=5, disp="off")
        forecast = res.forecast(horizon=1).variance.iloc[-1, 0]
        return {"variance_forecast": forecast}

# Montar el endpoint en Serve
GARCHService.deploy()

# Exponer FastAPI para verificar si está corriendo
@app.get("/health")
def health_check():
    return {"status": "garch model running"}
