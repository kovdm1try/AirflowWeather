# airflow-weather

## Stack
- **Apache Airflow 3.1.3** (API-server + scheduler + dag-processor)
- **Docker / Docker Compose**
- **PostgreSQL 13** (metadata DB Airflow)
- `requests`, `pandas`

## Структура репозитория

```text
airflow-weather/
├─ dags/
│  ├─ test_dag.py          # простой тестовый DAG (hello world)
│  └─ weather_dag.py       # основной DAG для сбора погоды
├─ logs/                   # логи Airflow + CSV
├─ plugins/                # кастомные плагины Airflow
├─ .env                    # переменные окружения
├─ docker-compose.yaml     # оркестрация сервисов Airflow + Postgres
├─ Dockerfile              # билд кастомного образа Airflow
├─ requirements.txt        # Python-зависимости
└─ README.md
````

## 1. Подготовка

### .env

В корне проекта должен лежать файл `.env`.
Пример:

```env
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin(имя пользователя):admin(роль)
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager

AIRFLOW__API_AUTH__JWT_SECRET=<любая длинная ключ строка>
AIRFLOW__CORE__FERNET_KEY=<любая длинная ключ строка>

AIRFLOW_UID=50000
AIRFLOW_GID=0
```
---
```bash
# Так можно сгенерировать ключ строку
docker run --rm my-airflow:3.1.3 python -c "from cryptography.fernet import Fernet; print(Fern
et.generate_key().decode())"
---
```

## 2. Запуск инфраструктуры

Из корня репозитория:
```bash
docker compose up --build
```

После успешного старта:
* Web-интерфейс Airflow будет доступен по адресу:
  [http://localhost:8080](http://localhost:8080)
* Логин/пароль (simple auth): 
    1. Логин указывается в `.env` файле
    2. Пароль можно узнать с помощью команды:
  ```bash
    docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated                     
    ```
---
Остановить и очистить контейнеры/тома:
```bash
docker compose down -v
```
---

## 3. Настройка соединения с OpenWeatherMap

1. Зарегистрироваться на [https://openweathermap.org/api](https://openweathermap.org/api) и получить **API key**.

2. В Airflow UI перейти в **Admin → Connections**.

3. Создать новое соединение:

   * **Connection ID**: `openweather-api`
   * **Connection Type**: `HTTP`
   * Остальные поля можно оставить пустыми.
   * В блоке **Extra Fields JSON** указать:

     ```json
     {
       "api-key": "ВАШ_API_КЛЮЧ"
     }
     ```

4. Сохранить соединение.

Ключ в коде DAG не хранится – используется только через Connection.

## `weather_pipeline` (dags/weather_dag.py)

Основной DAG задания.
Пайплайн разбит на несколько задач:

1. **get_api_key** – читает API-ключ из соединения `openweather-api`.
2. **fetch_weather** – делает запросы в OpenWeatherMap для списка городов
   (например: `["Vladivostok,ru", "Khabarovsk,ru", "Blagoveshchensk,ru"]`),
   собирает сырые данные о погоде.
3. **preprocess** – преобразует результат в `pandas.DataFrame`, выполняет
   простую предобработку и сериализует его.
4. **save_data** – десериализует DataFrame и сохраняет его в CSV:

   ```text
   /opt/airflow/logs/weather_YYYY-MM-DD.csv
   ```

   На хост-машине файл появляется в:

   ```text
   ./logs/weather_YYYY-MM-DD.csv
   ```

DAG настроен на выполнение по расписанию (например, `@hourly`), а также
может быть запущен вручную через UI.

## 5. Логи и результаты

Логи всех задач доступны:

* в UI: вкладка **Grid → Task → Logs**
* на диске (смонтировано в проект):

  ```text
  logs/
    dag_id=weather_pipeline/
      run_id=.../
        task_id=fetch_weather/attempt=1.log
        task_id=preprocess/attempt=1.log
        task_id=save_data/attempt=1.log
  ```

CSV-файлы с погодой также лежат в директории `logs/`:

```text
logs/weather_2025-11-28.csv
```
