FROM apache/airflow:latest-python3.9
COPY requirements.txt .
COPY dags/ .
RUN pip install -r requirements.txt