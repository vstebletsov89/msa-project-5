# Task 3. Distributed Scheduling с Kubernetes CronJob

POC ежедневной выгрузки данных из PostgreSQL в CSV-файл по расписанию через `k8s CronJob`.

Реализован **простой экспорт одной таблицы** — `shipments`, согласно заданию.

## Состав артефактов

```
task-3/results
├── README.md                      # этот файл
├── run-demo.sh                    # «один скрипт — всё развёрнуто и запущено»
├── Dockerfile                     # образ python:3.13-slim, non-root
├── requirements.txt               # psycopg[binary]
├── exporter/
│   └── export.py                  # экспортёр: SELECT * FROM shipments → CSV
└── k8s/
    ├── namespace.yaml
    ├── secret.yaml                # креды к Postgres
    ├── postgres-configmap.yaml    # init.sql, монтируется в Postgres
    ├── postgres-pvc.yaml          # PVC для данных Postgres
    ├── postgres-deployment.yaml
    ├── postgres-service.yaml
    ├── exporter-cronjob.yaml      # CronJob: 0 20 * * *, Europe/Moscow
    └── exporter-manual-job.yaml   # ручной Job для демонстрации
```

## Как работает

1. `CronJob` `shipments-exporter` ежедневно в **20:00 Europe/Moscow** создаёт `Job` → `Pod`.
2. Под выполняет `python /app/exporter/export.py`:
    - подключается к Postgres (`postgres:5432`);
    - через server-side cursor читает таблицу `shipments`;
    - пишет CSV в `/exports/shipments_<timestamp>.csv` (том `emptyDir`).
3. Параметры CronJob:
    - `concurrencyPolicy: Forbid` — защита от параллельного запуска;
    - `backoffLimit: 3` — retry при падении;
    - `activeDeadlineSeconds: 1800` — hard-timeout 30 минут;
    - `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 7`.

> В проде вместо `emptyDir` — выгрузка в S3/GCS, креды Postgres — из ExternalSecret/Vault.
> Здесь всё намеренно сведено к локальному тому, чтобы демо в MiniKube запускалось «из коробки».

## Быстрый старт (одной командой)

```bash
cd task-3/results
chmod +x run-demo.sh
./run-demo.sh
```

Скрипт:

1. поднимает MiniKube (если не запущен);
2. собирает Docker-образ `shipments-exporter:v1.0.0` **внутри docker-демона MiniKube** (без registry);
3. применяет все манифесты;
4. дожидается готовности Postgres (init.sql с 5000 строк применяется автоматически);
5. удаляет предыдущий ручной Job (если был) и запускает новый;

Ожидаемый «хвост» лога:

```
... INFO shipments-exporter - Starting export of table 'shipments' to /exports/shipments_20260419_174501.csv
... INFO shipments-exporter - Export finished: rows=5000, bytes=..., file=/exports/shipments_20260419_174501.csv
```

## Ручные команды (если нужно)

```bash
# Проверить расписание
kubectl -n shipments-export get cronjob shipments-exporter

# Посмотреть историю запусков
kubectl -n shipments-export get jobs,pods

# Запустить Job вручную из CronJob
kubectl -n shipments-export create job \
  --from=cronjob/shipments-exporter shipments-exporter-manual

# Логи последнего пода
kubectl -n shipments-export logs -l app=shipments-exporter --tail=200

# Полная очистка
kubectl delete namespace shipments-export
```
