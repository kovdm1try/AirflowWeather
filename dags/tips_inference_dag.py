import os
import logging
from datetime import datetime

import pandas as pd
import joblib
import matplotlib.pyplot as plt

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

ARTIFACTS_DIR = "/opt/airflow/logs/artifacts"
REPORTS_DIR = f"{ARTIFACTS_DIR}/reports"
PLOTS_DIR = f"{ARTIFACTS_DIR}/plots"


def create_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)


@dag(
    dag_id="tips_inference_pipeline",
    start_date=datetime(2026, 1, 8),
    schedule=None,
    catchup=False,
    tags=["lab", "inference", "viz"],
)
def tips_inference_pipeline():
    @task
    def read_conf() -> dict[str, str]:
        context = get_current_context()
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

        model_path = conf.get("model_path")
        processed_path = conf.get("processed_path")
        tag = conf.get("tag")

        if not model_path or not processed_path:
            raise AirflowException(f"Bad dag_run.conf: {conf}")

        logging.info("Inference conf: model=%s processed=%s tag=%s", model_path, processed_path, tag)
        return {"model_path": model_path, "processed_path": processed_path, "tag": tag}

    @task
    def run_inference(payload: dict) -> dict[str, str]:
        create_dirs()

        model_path = payload["model_path"]
        processed_path = payload["processed_path"]
        tag = payload["tag"]

        df = pd.read_csv(processed_path)
        logging.info("Loaded processed data: %s shape=%s", processed_path, df.shape)

        model = joblib.load(model_path)
        logging.info("Loaded model: %s", model_path)

        X = df.drop(columns=["tip"])
        y_true = df["tip"]

        y_pred = model.predict(X)

        preds_path = f"{REPORTS_DIR}/inference_predictions_{tag}.csv"
        out = pd.DataFrame({"y_pred": y_pred})

        if y_true is not None:
            out.insert(0, "y_true", y_true.values)

        out.to_csv(preds_path, index=False)
        logging.info("Saved inference predictions to %s", preds_path)

        return {"preds_path": preds_path, "tag": tag}

    @task
    def visualize(result: dict) -> str:
        create_dirs()

        preds_path = result["preds_path"]
        tag = result["tag"]

        df = pd.read_csv(preds_path)
        plot_path = f"{PLOTS_DIR}/inference_plot_{tag}.png"

        plt.figure()
        plt.scatter(df["y_true"], df["y_pred"])
        plt.xlabel("y_true (tip)")
        plt.ylabel("y_pred (tip)")
        plt.title("Inference")


        plt.tight_layout()
        plt.savefig(plot_path, dpi=160)
        plt.close()

        logging.info("Saved plot to %s", plot_path)
        return plot_path

    conf = read_conf()
    res = run_inference(conf)
    visualize(res)


tips_inference_pipeline_instance = tips_inference_pipeline()
