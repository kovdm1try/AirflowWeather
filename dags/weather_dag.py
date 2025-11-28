from datetime import datetime
import requests
import pandas as pd
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

CITIES = ["Blagoveshchensk", "Khabarovsk", "Vladivostok"]
CITIES_CODE = {
    "Blagoveshchensk": "ru",
    "Khabarovsk": "ru",
    "Vladivostok": "ru",
}


@dag(
    dag_id="weather_pipeline",
    start_date=datetime.now(),
    schedule="@hourly",
    catchup=False,
    tags=["weather", "openweather"],
)
def weather_pipeline():
    @task
    def get_api_key():
        logging.info("Getting API key from Airflow Connection...")
        conn = BaseHook.get_connection("openweather-api")
        api_key = conn.extra_dejson.get("api-key")

        if not api_key:
            raise ValueError("API key not found in Connection 'openweather-api'")
        return api_key

    @task
    def fetch_weather(api_key: str):
        logging.info("Get data about cities: %s", CITIES)

        responses = []

        for city in CITIES:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{CITIES_CODE[city]}&APPID={api_key}&units=metric"
            r = requests.get(url)
            print(r.status_code, r.json())
            if r.status_code == 200:
                data = r.json()

                responses.append({
                    "city": city,
                    "temp": data["main"]["temp"],
                    "feels_like": data["main"]["feels_like"],
                    "humidity": data["main"]["humidity"],
                    "timestamp": datetime.utcnow().isoformat(),
                })

                logging.info("City %s: %s", city, responses[-1])
            else:
                responses.append({
                    "city": city,
                    "temp": None,
                    "feels_like": None,
                    "humidity": None,
                    "timestamp": datetime.utcnow().isoformat(),
                })

                logging.info("City %s: %s", city, 'Can\'t fetch data')

        return responses

    @task
    def preprocess(rows: list):
        logging.info("Converting into pandas DataFrame")
        df = pd.DataFrame(rows)
        logging.info("DataFrame shape = %s", df.shape)
        return df.to_json(orient="records")

    @task
    def save_data(json_df: str):
        df = pd.read_json(json_df)

        path = f"/opt/airflow/logs/weather_{datetime.utcnow().date()}.csv"
        df.to_csv(path, index=False)

        logging.info("DataFrame saved into %s", path)
        return path

    api_key = get_api_key()
    rows = fetch_weather(api_key)
    df = preprocess(rows)
    save_data(df)


weather_pipeline_instance = weather_pipeline()
