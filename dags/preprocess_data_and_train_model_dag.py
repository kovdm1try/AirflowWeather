import os
import json
import logging
from datetime import datetime

import pandas as pd
import joblib

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

ARTIFACTS_DIR = "/opt/airflow/logs/artifacts"
PROCESSED_DIR = f"{ARTIFACTS_DIR}/processed"
MODELS_DIR = f"{ARTIFACTS_DIR}/models"
REPORTS_DIR = f"{ARTIFACTS_DIR}/reports"


def create_dirs() -> None:
    for dir_path in [PROCESSED_DIR, MODELS_DIR, REPORTS_DIR]:
        os.makedirs(dir_path, exist_ok=True)


@dag(
    dag_id="preprocess_data_and_train_model_pipeline",
    start_date=datetime(2026, 1, 8),
    schedule=None,
    catchup=False,
    tags=["lab", "validate", "preprocess", "train"],
)
def preprocess_data_and_train_model_pipeline():
    @task
    def read_data() -> dict[str, str]:
        context = get_current_context()
        conf = {}

        if context.get('dag_run'):
            conf = context.get('dag_run').conf

        data_path = conf.get('csv_path')
        if not data_path:
            raise AirflowException(f"No csv_path configured in DAG context.")

        logging.info(f"Receive data from {data_path}")
        return {'data_path': data_path}

    @task
    def validate_preprocess_data(conf) -> dict[str, str]:
        create_dirs()
        data_path = conf['data_path']

        df = pd.read_csv(data_path)
        logging.info(f"Read data from {data_path}")

        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str).str.strip()

        correct_columns = {"total_bill", "tip", "sex", "smoker", "day", "time", "size"}
        if correct_columns - set(df.columns):
            raise AirflowException(f"The following columns are not present in {data_path}")
        df = df.loc[:, df.columns.intersection(correct_columns)]

        if df.empty:
            raise ValueError(f"{data_path} is empty")
        if df["total_bill"].isna().any() or df["tip"].isna().any():
            raise ValueError("Found NaN in critical columns total_bill/tip")
        if (df["total_bill"] <= 0).any():
            raise ValueError("Found non-positive total_bill")
        if (df["tip"] < 0).any():
            raise ValueError("Found negative tip")
        logging.info(f"Validation - PASSED")

        df = df.drop_duplicates()

        tag = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        processed_path = f"{PROCESSED_DIR}/data_processed_{tag}.csv"
        df.to_csv(processed_path, index=False)
        logging.info("Saved processed data to %s", processed_path)

        return {'processed_path': processed_path}

    @task
    def train_preprocess_data(conf):
        df = pd.read_csv(conf['processed_path'])

        target = "tip"
        X = df.drop(columns=[target])
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        num_columns = X.select_dtypes(include=["int64", "float64"]).columns.tolist()
        cat_columns = X.select_dtypes(include=["object", "bool"]).columns.tolist()

        num_pipline = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ('scaler', StandardScaler()),
        ])

        cat_pipline = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ])

        preproc = ColumnTransformer(
            transformers=[
                ("numerical", num_pipline, num_columns),
                ("categorical", cat_pipline, cat_columns),
            ],
            remainder="drop",
        )

        model = RandomForestRegressor(
            n_estimators=300,
            random_state=42,
            n_jobs=-1,
        )

        pipeline = Pipeline(steps=[
            ("preprocess", preproc),
            ("model", model),
        ])

        logging.info("Start training model...")
        pipeline.fit(X_train, y_train)

        logging.info("Start inference on test split...")
        y_pred = pipeline.predict(X_test)

        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        metrics = {
            "mae": float(mae),
            "r2": float(r2),
            "rows_train": int(len(X_train)),
            "rows_test": int(len(X_test)),
            "timestamp": datetime.utcnow().isoformat(),
        }
        logging.info("Metrics: %s", metrics)

        create_dirs()
        tag = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

        model_path = f"{MODELS_DIR}/tips_model_{tag}.joblib"
        joblib.dump(pipeline, model_path)
        logging.info("Saved model to %s", model_path)

        metrics_path = f"{REPORTS_DIR}/tips_metrics_{tag}.json"
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        logging.info("Saved metrics to %s", metrics_path)

        preds_path = f"{REPORTS_DIR}/tips_predictions_{tag}.csv"
        pd.DataFrame({"y_true": y_test.values, "y_pred": y_pred}).to_csv(preds_path, index=False)
        logging.info("Saved predictions to %s", preds_path)

        return {
            "tag": tag,
            "model_path": model_path,
            "preds_path": preds_path,
            "metrics_path": metrics_path,
            "processed_path": conf["processed_path"],
        }

    data_conf = read_data()
    processed_conf = validate_preprocess_data(data_conf)
    train_preprocess_data(processed_conf)

    train_out = train_preprocess_data(processed_conf)

    trigger_inference = TriggerDagRunOperator(
        task_id="trigger_inference_dag",
        trigger_dag_id="tips_inference_pipeline",
        conf={
            "tag": "{{ ti.xcom_pull(task_ids='train_preprocess_data')['tag'] }}",
            "model_path": "{{ ti.xcom_pull(task_ids='train_preprocess_data')['model_path'] }}",
            "processed_path": "{{ ti.xcom_pull(task_ids='train_preprocess_data')['processed_path'] }}",
        },
        wait_for_completion=False,
    )

    train_out >> trigger_inference


preprocess_data_and_train_model_pipline_instance = preprocess_data_and_train_model_pipeline()
