# POC — Apache Airflow для Task 1

Локальный стенд для демонстрации пакетной обработки данных на Apache Airflow.

## Состав стенда

| Сервис               | Назначение                                          | URL / порт                |
|----------------------|-----------------------------------------------------|---------------------------|
| `airflow-webserver`  | Airflow Web UI                                      | http://localhost:8080 (admin / admin) |
| `airflow-scheduler`  | Планировщик Airflow                                 | —                         |
| `postgres`           | Метаданные Airflow                                  | внутренний                |
| `source-postgres`    | Источник данных (таблица `orders`)                  | localhost:5433            |
| `mailhog`            | SMTP-ловушка для email-уведомлений                  | http://localhost:8025     |

DAG: `dags/marketing_batch_pipeline.py` — читает CSV и таблицу в PostgreSQL,
выполняет ветвление по объёму данных, отправляет email при успехе/неуспехе,
использует retry с экспоненциальным backoff.

## Запуск

```bash
cd task-1/results
docker compose up -d
```