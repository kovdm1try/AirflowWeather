## Запуск
```bash
# 1. Подготовить директории для DAG'ов, логов и плагинов
mkdir -p dags logs plugins data

# 2. Собрать кастомный образ Airflow (my-airflow:3.1.3)
docker compose build

# 3. Запустить только Postgres
docker compose up -d postgres

# 4. Инициализировать / мигрировать БД Airflow в Postgres
docker compose run --rm webserver airflow db migrate

# 5. Запустить весь стек (webserver + scheduler + postgres)
docker compose up -d

# 6. Проверить, что все контейнеры в статусе Up
docker compose ps
````

## Доступ к веб-интерфейсу

После запуска веб-интерфейс доступен по адресу:

* [http://localhost:8080](http://localhost:8080)

Airflow 3 по умолчанию использует **Simple auth manager**, пользователи и пароли создаются автоматически
на основе переменной окружения `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` (см. `.env`).

Чтобы посмотреть сгенерированные пароли для пользователей:

```bash
docker compose exec webserver sh -lc 'echo $AIRFLOW_HOME'
docker compose exec webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

## Остановка

```bash
docker compose down
```

(если нужно удалить также данные Postgres — `docker compose down -v`)

```
::contentReference[oaicite:0]{index=0}
```
