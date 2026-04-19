# Task 4 — Результаты

## Артефакты
- [ADR](./ADR.md) — обоснование Spring Batch vs Airflow / K8s Job / Spark / NiFi.
- [C4 To-Be](./to_be_c4.puml) — архитектура с внедрённым Spring Batch компонентом.
- Исправленный шаблон Java-приложения — `task-4/initial/`.
- Демонстрация — `./screenshots/`.

## Краткое описание POC
Spring Boot 3 + Spring Batch 5:
- `FlatFileItemReader` читает `product-data.csv`.
- `ProductItemProcessor` обогащает поле `productData` значением из `loyality_data` по `productSku`.
- `JdbcBatchItemWriter` UPSERT-ит результат в `products`.
- Chunk = 10, retry=3 (TransientDataAccessException), skip=5 (FlatFileParseException).
- JobRepository хранится в том же PostgreSQL (`BATCH_*` таблицы) — поддерживает restart.

## Запуск
```bash
cd task-4/initial
docker compose up --build
```

```bash
docker compose exec postgresdb psql -U postgres -d productsdb -c "SELECT * FROM products ORDER BY productid;"

docker compose exec postgresdb psql -U postgres -d productsdb \
  -c "SELECT * FROM loyality_data ORDER BY productsku;"
```

```bash
docker compose exec postgresdb psql -U postgres -d productsdb \
-c "SELECT job_execution_id, status, exit_code, start_time, end_time FROM BATCH_JOB_EXECUTION;"
```