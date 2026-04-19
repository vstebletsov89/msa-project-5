# ADR-1. Выбор решения для пакетной обработки данных

## Общая информация

1. **Родительский артефакт** — Архитектура платформы аналитики маркетингового отдела 
2. **Автор ADR** — Вячеслав Стеблецов
3. **Статус** — Принят.
4. **Согласующие** — Архитектор решения, Tech Lead Data Platform, Руководитель маркетингового отдела, DevOps-инженер.

## Контекст

Маркетинговый отдел расширяет обработку клиентских данных и формирование отчётов. Источники данных разнородны:

- CSV-файлы со статусами доставок (файловое хранилище);
- PostgreSQL — заказы, платежи, расширенные данные о пользователях;
- Kafka — цепочки событий модификации заказов.

Текущее решение не справляется ни с нагрузкой (~1 млн записей за запуск пайплайна), ни с требуемой функциональностью.

**Ключевые требования к решению:**

- гибкое построение пайплайнов (DAG, ветвления, условные операторы);
- event-triggers и расписания;
- интеграция с BigQuery, Amazon Redshift, Apache Kafka, Apache Spark (желательно — готовые модули/провайдеры);
- из коробки: retry-политика, fallback-логика, email-уведомления;
- встроенный мониторинг и алертинг;
- возможность локального развёртывания (dev/POC) с последующим переносом в облако (GCP/AWS);
- целевой объём — ~1 млн записей на один запуск.

Вне scope данного ADR: выбор облака, схемы хранилищ, модели данных отчётности, SLA по задержке доставки данных в DWH.

## Рассмотренные варианты

### Вариант №1 «Apache Airflow»

Оркестратор пайплайнов данных на основе Python. Пайплайн описывается как DAG, из коробки поддерживает retry, SLA, уведомления (email, Slack и др.), ветвления (`BranchPythonOperator`, `ShortCircuitOperator`), sensors для event-driven триггеров, богатый Web UI и метрики (StatsD/Prometheus через exporter).

Имеет готовые provider-пакеты: `apache-airflow-providers-google` (BigQuery), `apache-airflow-providers-amazon` (Redshift, S3), `apache-airflow-providers-apache-kafka`, `apache-airflow-providers-apache-spark`, `apache-airflow-providers-postgres`.

Облачные managed-варианты: Google Cloud Composer, AWS MWAA, Astronomer.


### Вариант №2 «Prefect 2.x»

Python-оркестратор с декларативным API (`@flow`, `@task`), dynamic DAG, богатым UI (Prefect Cloud / self-hosted Prefect Server). Имеет retry, уведомления (через `prefect-email`, Slack-блоки), интеграции `prefect-gcp`, `prefect-aws`, `prefect-dask`, `prefect-spark`. Интеграции с Kafka — через сторонние библиотеки/самописный код.

### Вариант №3 «Dagster»

Asset-oriented оркестратор с сильной типизацией, встроенным каталогом ассетов, observability. Есть интеграции `dagster-gcp` (BigQuery), `dagster-aws` (Redshift), `dagster-spark`, `dagster-pyspark`. Kafka — через community-модули. Retry, sensors (event-triggers), email-уведомления поддерживаются.

### Вариант №4 «Apache NiFi»

Flow-based инструмент data-ingestion с drag-and-drop UI. Хорошо подходит для потоковой и near-real-time интеграции, имеет процессоры для Kafka, JDBC, S3. Слабее как оркестратор batch-отчётности: зависимости между шагами выражаются через очереди, а не DAG; тестируемость и code-review flow-файлов хуже, чем у Python-кода.

### Вариант №5 «Luigi»

Python-оркестратор от Spotify, строит DAG из Python-таргетов. Стабильный, но слаборазвиваемый: нет полноценного UI с управлением запусками, слабые механизмы event-triggers, retry и уведомлений, ограниченный набор готовых интеграций с облачными DWH.

## Сравнение альтернатив

| Вариант решения         | Преимущества                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Недостатки                                                                                                                                                                                                                                                                                     |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| №1 «Apache Airflow»     | - Официальные провайдеры для BigQuery, Redshift, Kafka, Spark, Postgres, S3<br>- Из коробки: `retries`, `retry_delay`, `email_on_failure`, `email_on_retry`, SLA, callbacks (`on_failure_callback` для fallback-логики)<br>- Ветвления: `BranchPythonOperator`, `ShortCircuitOperator`, TaskFlow API<br>- Event-triggers: `TriggerDagRunOperator`, Sensors, Dataset-driven scheduling, Deferrable operators<br>- Managed-сервисы в облаке: Cloud Composer (GCP), MWAA (AWS), Astronomer<br>- Крупное сообщество, зрелость | - Выше порог входа (Scheduler, Webserver, Workers, метаданные)<br>- Требует отдельную БД метаданных<br>- Конфигурация для high-load требует аккуратной настройки (Celery/Kubernetes Executor)                                                                                                                |
| №2 «Prefect 2.x»        | - Современный Python API, удобная локальная разработка<br>- Retry, уведомления, интеграции с GCP/AWS<br>- Dynamic workflows                                                                                                                                                                                                                                                                                                                                                                                           | - Нет официального provider’а для Kafka<br>- Интеграции с Spark ограничены по сравнению с Airflow<br>- Меньше managed-вариантов в облаках (в основном Prefect Cloud)<br>- Сообщество и экосистема меньше                                                                                                |
| №3 «Dagster»            | - Asset-oriented подход, сильная типизация<br>- Хороший UI, observability, data lineage<br>- Интеграции с BigQuery, Redshift, Spark                                                                                                                                                                                                                                                                                                                                                                                    | - Парадигма ассетов требует перестройки мышления команды<br>- Kafka-интеграции — преимущественно community<br>- Меньшая зрелость managed-решений у крупных облачных провайдеров                                                                                                                           |
| №4 «Apache NiFi»        | - Визуальное построение потоков, быстрый старт для ingestion<br>- Хорошо работает с Kafka, JDBC, файлами                                                                                                                                                                                                                                                                                                                                                                                                          | - Слабая поддержка сложной batch-оркестрации (ветвления, условия)<br>- Хуже тестируемость и code review (XML flow-файлы)<br>- Слабые интеграции с BigQuery/Redshift/Spark «из коробки»                                                                                                             |
| №5 «Luigi»              | - Простая модель, минимум инфраструктуры                                                                                                                                                                                                                                                                                                                                                                                                                                                                | - Ограниченный UI и управление запусками<br>- Нет полноценных event-triggers и уведомлений из коробки<br>- Мало готовых интеграций с облачными DWH и Kafka<br>- Фактически в режиме поддержки, медленное развитие                                                                                                |

### Покрытие требований выбранным решением (Airflow)

- **Интеграции из коробки:**
    - BigQuery — `apache-airflow-providers-google` (`BigQueryInsertJobOperator`, `BigQueryHook`);
    - Redshift — `apache-airflow-providers-amazon` (`RedshiftSQLOperator`, `RedshiftDataOperator`, `S3ToRedshiftOperator`);
    - Kafka — `apache-airflow-providers-apache-kafka` (`ConsumeFromTopicOperator`, `ProduceToTopicOperator`, `AwaitMessageSensor` для event-triggers);
    - Spark — `apache-airflow-providers-apache-spark` (`SparkSubmitOperator`, `SparkKubernetesOperator`) + `SparkKubernetesSensor`.
- **Ветвления/условные операторы:** `BranchPythonOperator`, `ShortCircuitOperator`, `@task.branch`, `trigger_rule`.
- **Event-triggers:** Sensors (FileSensor, SqlSensor, `AwaitMessageSensor`), Datasets / Data-aware scheduling, `TriggerDagRunOperator`.
- **Retry / fallback / email:** параметры `retries`, `retry_delay`, `retry_exponential_backoff`, `email_on_failure`, `email_on_retry`, `on_failure_callback`, `on_success_callback`, `EmailOperator`.
- **Мониторинг:** Web UI (Gantt, Graph, Logs), метрики в StatsD/Prometheus, интеграции с Grafana, ELK.

### Развёртывание в облаке

- **GCP:** Cloud Composer — managed Airflow, интегрирован с BigQuery, GCS, Pub/Sub, Dataproc (Spark).
- **AWS:** Amazon MWAA — managed Airflow, нативная работа с S3, Redshift, MSK (Kafka), EMR (Spark).
- **Kubernetes:** официальный Helm-chart `apache-airflow/airflow` с `KubernetesExecutor` — одинаковый способ развёртывания в любом облаке или on-prem.
- Локальная среда (этот POC) использует `docker-compose` с `LocalExecutor` и Postgres в роли metadata DB; DAG и конфигурация полностью переносятся в Composer/MWAA/Helm без изменений.

## Принятое решение

Принят **Вариант №1 «Apache Airflow»**.

**Обоснование:**

1. Покрывает все ключевые требования «из коробки»: retry с backoff, fallback через callbacks, email-уведомления, ветвления, event-triggers через Sensors и Datasets.
2. Имеет официальные provider-пакеты для BigQuery, Redshift, Kafka и Spark — это сокращает объём собственного кода и риски интеграции.
3. Имеет зрелые managed-предложения во всех целевых облаках (Cloud Composer, MWAA) и официальный Helm-chart — это снижает риск привязки к одному вендору.
4. Python-first подход упрощает тестирование, code review и переиспользование кода между командами.
5. Объём в ~1 млн записей за запуск легко обслуживается при корректной делегации тяжёлых вычислений во внешние системы (Spark, BigQuery, Redshift) — Airflow здесь играет роль оркестратора, а не обработчика данных, что соответствует лучшим практикам.
6. Большое сообщество и длительная история промышленной эксплуатации снижают операционные риски по сравнению с Prefect/Dagster/Luigi/NiFi.