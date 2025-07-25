FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY service.py .
COPY data/ ./data/

CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8000"]
