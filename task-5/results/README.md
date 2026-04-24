# TradeWare Stock ETL — Monitoring, Logging & Alerting

Проект демонстрирует мониторинг, логирование и оповещения для Spring Batch ETL-приложения.

## Используемый стек

- **Spring Batch + Micrometer** — генерация метрик
- **Prometheus** — сбор метрик
- **Grafana** — дашборды и алерты
- **Filebeat → Logstash → Elasticsearch → Kibana (ELK)** — централизованное логирование
- **Docker Compose** — локальное развёртывание

---

## Запуск проекта

```bash
docker compose up --build
```

## Доступ к сервисам

После запуска доступны:

| Сервис        | URL |
|---------------|-----|
| Grafana       | http://localhost:3000 |
| Prometheus    | http://localhost:9090 |
| Kibana        | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| App metrics   | http://localhost:8080/actuator/prometheus |

---

## Запуск ETL job

```bash
docker compose up app
```

## Просмотр метрик (Prometheus)

http://localhost:9090

Примеры метрик:

stock_etl_last_run_status
stock_etl_last_run_duration_seconds
stock_etl_last_run_skip_count
process_cpu_usage
jvm_memory_used_bytes

## Grafana (дашборды и алерты)

http://localhost:3000

логин/пароль: `admin / admin`

### Что посмотреть

- Dashboard: **Stock ETL Batch Dashboard**

Метрики:

- статус последнего запуска
- длительность job (SLA)
- skip count
- CPU / JVM memory

---

##  Alerts

Настроены алерты:

- `StockEtlLastRunFailed`
- `StockEtlSlowRun`
- `StockEtlHighSkipCount`
