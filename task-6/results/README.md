# Task 6 — Spring Batch + Tracing

## Что добавлено относительно Task 5

- `spring.batch.job.enabled=false` — Job больше не запускается при старте.
- REST endpoint `POST /api/jobs/import-products?fileName=...` для запуска Job.
- Подключён Micrometer Tracing (Brave) — `traceId` и `spanId` автоматически
  попадают в MDC.
- `MdcUriFilter` кладёт URI запроса в MDC под ключом `uri`.
- `logback-spring.xml` пишет JSON-логи с полями `traceId`, `spanId`, `uri`,
  которые через Filebeat → Logstash → Elasticsearch попадают в Kibana.

  
## Стек трейсинга

| Слой               | Решение                                       |
|--------------------|-----------------------------------------------|
| Trace context      | Micrometer Tracing + Brave (B3)               |
| MDC enrichment     | Авто (Spring Boot) + `MdcUriFilter` для `uri` |
| Формат логов       | JSON (`logstash-logback-encoder`)             |
| Доставка           | Filebeat → Logstash → Elasticsearch           |
| Просмотр           | Kibana (`http://localhost:5601`)              |

## Запуск

```bash
# 1. Поднять стек (PostgreSQL, app, Prometheus, Grafana, ELK, Filebeat)
docker compose up -d --build

# 2. Запустить клиента
cd ../client
py run.py
```
###  Посмотреть логи в Kibana
- Открыть `http://localhost:5601`
- Используем view из предыдущего задания
- Добавить колонки: `traceId`, `spanId`, `uri`, `message`, `level`
- Скопировать `traceId` из вывода клиента и отфильтровать:
  `traceId : "<id>"` 
