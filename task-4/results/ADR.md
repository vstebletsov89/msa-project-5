# ADR-004. Внедрение Spring Batch как ETL-решения для обработки отчётов по остаткам

- **Статус:** Accepted
- **Дата:** 2026-04-19
- **Авторы:** Вячеслав Стеблецов
- **Контекст:** Задание 4 проектной работы 5 спринта MSA.

## Контекст

TradeWare обрабатывает ~400 000 строк/день отчётов по остаткам, в пик до x2–x3.
Текущий монолит на Java 11 / WildFly обрабатывает CSV построчно в online-режиме,
что приводит к:

- деградации UI ERP при пиках (100–150 параллельных загрузок);
- отсутствию retry/skip — сбой строки = повторная загрузка всего файла;
- отсутствию централизованного мониторинга шагов обработки;
- невозможности масштабировать ETL независимо от online-слоя.

Бизнес-цели:
- среднее время обработки 2 000 строк ≤ 30 сек;
- 100–150 параллельных загрузок в пик;
- централизованные логи/метрики;
- плавная миграция в микросервисную архитектуру.

## Рассматриваемые альтернативы

| Критерий / Решение | **Spring Batch** | Apache Airflow | K8s Job + plain Java | Apache Spark | Apache NiFi |
|---|---|---|---|---|---|
| Chunk-based обработка из коробки | ✅ | ❌ (оркестратор) | ❌ | ✅ | ✅ |
| Restart / skip / retry on failure | ✅ native | ⚠️ task-level | ❌ | ⚠️ | ✅ |
| Интеграция с Java / Spring-экосистемой TradeWare | ✅ | ❌ (Python) | ✅ | ⚠️ | ❌ |
| Порог входа для команды (Java 11+) | Низкий | Высокий | Низкий | Высокий | Средний |
| Метрики Micrometer / Prometheus | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| Ресурсоёмкость | Низкая | Средняя | Низкая | **Высокая** | Средняя |
| Подходит под ~400k строк/день | ✅ | ✅ (как оркестратор) | ✅ | Избыточен | ✅ |
| Горизонтальное масштабирование | ✅ (partitioning / remote chunking) | ✅ | ⚠️ ручное | ✅ | ✅ |
| Готовность к облаку (GKE) | ✅ (Spring Boot image) | ✅ | ✅ | ✅ | ⚠️ |

## Решение

Использовать **Spring Batch 5 (Spring Boot 3)** как ETL-движок:

- `FlatFileItemReader` — чтение CSV из GCS (локально — classpath);
- `ItemProcessor` — обогащение данными `loyality_data` из PostgreSQL;
- `JdbcBatchItemWriter` — батч-вставка в `products`;
- chunk size = 10 (настраивается), `faultTolerant().retry(...).skip(...)`;
- JobRepository в PostgreSQL (таблицы `BATCH_*`) для restart/idempotency;
- запуск как Spring Boot контейнер в Kubernetes (CronJob / Job);
- метрики через Micrometer → Prometheus, логи → ELK.

## Последствия

**Плюсы:**
- Команда Java умеет поддерживать Spring-стек — минимум onboarding.
- Chunk + transaction boundary решают проблему построчных UPDATE.
- Restart «с места падения» — за счёт JobRepository.
- Легко перенести в GKE: Spring Boot uber-jar + Dockerfile уже есть.
- Готов к расширению: partitioned step для x2–x3 пика.

**Минусы / риски:**
- Для действительно «больших» данных (десятки млн) потребуется remote partitioning.
- Spring Batch не является оркестратором — расписание остаётся за k8s CronJob / Spring Cloud Data Flow.
- Мониторинг нужно настраивать отдельно (Micrometer + Prometheus).

## План имплементации (high-level)

1. POC-приложение (настоящее задание): CSV → enrich → Postgres.
2. Вынесение Job в отдельный Spring Boot сервис.
3. Образ в GCR, CronJob в GKE, креды через Secret Manager.
4. Metrics endpoint `/actuator/prometheus`, логи → stdout → ELK.
5. Масштабирование: partitioned step по диапазонам `productSku`.