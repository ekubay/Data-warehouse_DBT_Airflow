FROM python:3.8.1-slim-buster
FROM apache/airflow:2.2.0
COPY requirements.txt .
RUN pip install -r requirements.txt


