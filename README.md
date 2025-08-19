# Football-Data-Analytics
This Project base on python that crawls data from wikipedia using Apache Airflow, cleans it and pushs it into Azure Data Lakes for processing.

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Requirements](#requirements)
3. [Getting Started](#getting-started)
4. [Running the code With Docker](#running-the-code-with-docker)
5. [How It works](#how-it-works)

## System Architecture
![system-architecture.png](https://github.com/user-attachments/assets/05e28e91-b317-4153-b215-af89650bba05)

## Requirements
- Python 3.9 (Minimum)
- Docker
- PostgreSQL
- Apache Airflow 2 (Minimum)

## Getting Started
1. Clone the repository
```bash
git clone https://github.com/chayuthpong31/Football-Data-Analytics.git
```

2. Install Python dependencies.
```bash
pip install -r requirements.txt
```

## Running the Code With Docker
1. Start your services on Docker with
```bash
docker compose up -d
```

2. Trigger the DAG on the Airflow UI.

## How It Works
1. Fetches data from Wikipedia.
2. Cleans the data.
3. Transforms the data.
4. Pushes the data to Azure Data Lakes.