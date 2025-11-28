## Запуск

```bash
mkdir -p dags logs plugins

docker compose pull
docker compose up -d postgres
docker compose run --rm webserver airflow db init

docker compose run --rm webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
  
docker compose up -d
```