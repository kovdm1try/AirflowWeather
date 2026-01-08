import os
import logging
from datetime import datetime

import requests
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DATA_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
SAVE_DIR = "/opt/airflow/logs/csv"

@dag(
    dag_id="data_extract_pipeline",
    start_date=datetime(2026, 1, 8),
    schedule="@daily",
    catchup=False,
    tags=["lab", "extract", "data"],
)
def data_extract_pipline():
    @task
    def download_data() -> str:
        logging.info("Downloading dataset from: %s", DATA_URL)

        response = requests.get(DATA_URL)
        response.raise_for_status()

        os.makedirs(SAVE_DIR, exist_ok=True)
        data_filename = f"data_extract_dag.csv"
        data_filepath = os.path.join(SAVE_DIR, data_filename)

        with open(data_filepath, "wb") as f:
            f.write(response.content)

        logging.info("Dataset saved to %s", data_filepath)

        return data_filepath


    @task
    def check_data(path) -> str:
        logging.info("Checking dataset from: %s", DATA_URL)

        if pd.read_csv(path).empty:
            logging.info("Dataset is empty, skipping.")
            raise ValueError('Dataset is empty')

        return path

    p = download_data()
    checked_path = check_data(p)

    trigger_train = TriggerDagRunOperator(
        task_id="preprocess_data_and_train_model_pipeline",
        trigger_dag_id="preprocess_data_and_train_model_pipeline",
        conf={
            "csv_path": "{{ ti.xcom_pull(task_ids='check_data') }}",
        },
        wait_for_completion=False,
    )
    checked_path >> trigger_train

data_extract_pipline_result = data_extract_pipline()
