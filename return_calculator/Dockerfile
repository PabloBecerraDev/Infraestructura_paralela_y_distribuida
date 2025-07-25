FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY service.py .
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8003"]
