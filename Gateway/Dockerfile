FROM python:3.10-slim

WORKDIR /app
COPY service.py .

RUN pip install flask requests

CMD ["python", "service.py"]
