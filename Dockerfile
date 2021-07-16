FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim
ENV BIND 0.0.0.0:8000
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
