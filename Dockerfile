FROM apache/airflow:2.6.0-python3.9

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        pkg-config \
        libcairo2-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
