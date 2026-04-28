# План имплементации модуля B2B Price List Batch

Верхнеуровневый план реализации решения на базе **Kubernetes CronJob** (см. `ADR.md`).

## 1. Подготовка окружения

- Namespace `batch-jobs`, `ServiceAccount` с минимальными RBAC-правами.
- Секреты: `price-list-db` (read-replica Postgres) и `price-list-s3` (доступ к Object Storage) — либо Workload Identity / IRSA.
- `ConfigMap` с параметрами: bucket, префикс ключей (`pricelists/{YYYY-MM-DD}/{client_id}.csv`), формат (`csv`/`xls`), таймзона.

## 2. Разработка приложения-генератора

Тонкий контейнер (Python 3.13 + `psycopg` + `pandas`/`csv` + `boto3`/`google-cloud-storage`).

**Алгоритм:**

1. Прочитать конфиг и креды из env.
2. Выполнить `SELECT` с JOIN `clients → client_prices → products → categories` через server-side cursor.
3. Сгруппировать по `client_id` и сформировать отдельный файл на клиента.
4. Загрузить файл в Object Storage по ключу `pricelists/{date}/{client_id}.csv`.
5. Записать структурированные логи и push бизнес-метрик в Prometheus Pushgateway.
6. Exit code: `0` — успех, `!=0` — сбой (перезапуск по `backoffLimit`).

## 3. Контейнеризация и CI/CD

- `Dockerfile` на `python:3.13-slim`, non-root user, semver-теги (без `latest`).
- CI: линт → юнит-тесты → сборка → push в registry → обновление Helm-values через GitOps (ArgoCD/Flux).

## 4. Kubernetes CronJob

Ключевые параметры манифеста:

- `schedule: "0 6 * * *"`, `timeZone: "Europe/Moscow"`;
- `concurrencyPolicy: Forbid` — защита от параллельных запусков;
- `backoffLimit: 3` — retry-политика;
- `activeDeadlineSeconds: 1800` — hard-timeout 30 минут;
- `resources.requests/limits` — 200m CPU / 256Mi … 500m CPU / 512Mi;
- `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 7`.

## 5. Наблюдаемость

- **Логи:** stdout (JSON) → Filebeat → ELK, дашборд «Price List Batch» в Kibana.
- **Метрики:** `kube_job_status_failed`, `kube_cronjob_next_schedule_time` из kube-state-metrics + бизнес-метрики (`pricelist_clients_total`, `pricelist_rows_total`, `pricelist_duration_seconds`).
- **Алерты в Alertmanager → email/Slack:** падение Job, пропуск запуска, превышение SLA по длительности.

## 6. Безопасность

- `NetworkPolicy`: egress только до Postgres (5432) и Object Storage (443).
- БД-пользователь — read-only на нужные таблицы.
- Отдельный bucket с lifecycle-правилом (TTL 30 дней); выдача клиентам — через pre-signed URL с ограниченным временем жизни (B2B API, вне scope).

## 7. Тестирование и выкладка

- Unit-тесты рендера и группировки.
- Интеграционный прогон в dev-кластере (Postgres + MinIO), ручной запуск: `kubectl create job --from=cronjob/price-list-generator manual-run`.
- Нагрузочный прогон на синтетике (500 клиентов × 20k строк) — проверка SLA и лимитов.
- Staging → Production через GitOps; первую неделю — `suspend: true` + ручные запуски с наблюдением.
- Runbook: перезапуск, восстановление файлов, действия при падении.

## 8. Развитие в будущем

- Рост объёма → `parallelism: N` в `Job`, шардирование по `client_id % N`.
- Усложнение логики (несколько источников, event-triggers, DAG) → миграция на Airflow с переиспользованием того же контейнера через `KubernetesPodOperator`.
- Переход на on-demand генерацию → тот же образ запускается по API из очереди / B2B-портала.

CronJob манифест для POC:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: price-list-generator
  namespace: batch-jobs
spec:
  schedule: "0 6 * * *"          # ежедневно в 06:00
  timeZone: "Europe/Moscow"
  concurrencyPolicy: Forbid       # не запускать параллельно с предыдущим
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 7
  startingDeadlineSeconds: 600
  jobTemplate:
    spec:
      backoffLimit: 3             # retry-политика
      activeDeadlineSeconds: 1800 # hard-timeout 30 минут
      template:
        spec:
          serviceAccountName: price-list-generator
          restartPolicy: Never
          containers:
            - name: generator
              image: registry/.../price-list-generator:v1.0.0
              envFrom:
                - secretRef: { name: price-list-db }
                - secretRef: { name: price-list-s3 }
                - configMapRef: { name: price-list-config }
              resources:
                requests: { cpu: "200m", memory: "256Mi" }
                limits:   { cpu: "500m",    memory: "512Mi"   }
```