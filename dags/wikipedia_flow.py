from airflow import DAG
from datetime import datetime

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python import PythonOperator
from pipelines.wikipedia_pipelines import extract_wikipedia_data, transform_wikipedia_data, load_wikipedia_data

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        'owner':'Chayuthpong31',
        'start_date': datetime(year=2025, month=8, day=19)
    },
    schedule=None,
    catchup=False
)

# Extraction
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

# Transformation
transform_wikipedia_data = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    dag=dag
)

# Load 
load_wikipedia_data = PythonOperator(
    task_id="load_wikipedia_data",
    python_callable=load_wikipedia_data,
    dag=dag
)

extract_data_from_wikipedia>>transform_wikipedia_data>>load_wikipedia_data