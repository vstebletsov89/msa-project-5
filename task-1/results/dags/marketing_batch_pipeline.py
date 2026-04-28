"""
POC DAG for Task 1 — пакетная обработка маркетинговых данных.

Пайплайн:
  1. read_csv_deliveries       — чтение CSV-файла со статусами доставок.
  2. read_postgres_orders      — чтение заказов из PostgreSQL.
  3. analyze_and_branch        — анализ объединённых данных и ветвление.
  4a. process_high_volume      — ветка для «большого» объёма.
  4b. process_low_volume       — ветка для «малого» объёма.
  5. send_success_email        — email при успехе (через EmailOperator).
  Failure-callback             — email при неуспехе.

На шаге analyze_and_branch намеренно реализована нестабильная логика,
которая демонстрирует retry-политику Airflow (retries + retry_delay).
"""

from __future__ import annotations

import csv
import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

CSV_PATH = "/opt/airflow/data/delivery_statuses.csv"
NOTIFY_EMAIL = "marketing-ops@example.com"
HIGH_VOLUME_THRESHOLD = 5  # порог для ветвления


def _read_csv_deliveries(**context) -> int:
    path = Path(CSV_PATH)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    delivered = [r for r in rows if r["delivery_status"] == "DELIVERED"]
    log.info("CSV rows=%s, delivered=%s", len(rows), len(delivered))
    context["ti"].xcom_push(key="delivered_count", value=len(delivered))
    return len(delivered)


def _read_postgres_orders(**context) -> int:
    hook = PostgresHook(postgres_conn_id="source_postgres")
    records = hook.get_records(
        "SELECT order_id, user_id, amount FROM orders WHERE status = 'PAID';"
    )
    total_amount = float(sum(r[2] for r in records))
    log.info("Postgres paid orders=%s, total_amount=%.2f", len(records), total_amount)
    context["ti"].xcom_push(key="paid_orders_count", value=len(records))
    context["ti"].xcom_push(key="paid_orders_amount", value=total_amount)
    return len(records)


def _analyze_and_branch(**context) -> str:
    # Имитация нестабильности для демонстрации retry.
    # Управляется переменной окружения — по умолчанию включено.
    fail_prob = float(os.getenv("POC_FAIL_PROBABILITY", "0.4"))
    try_number = context["ti"].try_number
    if try_number == 1 and random.random() < fail_prob:
        raise RuntimeError(
            "Transient failure in analyze_and_branch — retry should recover"
        )

    ti = context["ti"]
    delivered = ti.xcom_pull(
        task_ids="read_csv_deliveries", key="delivered_count"
    ) or 0
    paid = ti.xcom_pull(
        task_ids="read_postgres_orders", key="paid_orders_count"
    ) or 0

    total = int(delivered) + int(paid)
    log.info("Joined volume: delivered=%s + paid=%s = %s", delivered, paid, total)

    if total >= HIGH_VOLUME_THRESHOLD:
        return "process_high_volume"
    return "process_low_volume"


def _process_high_volume(**_) -> None:
    log.info("HIGH VOLUME branch: делегируем тяжёлую агрегацию в Spark/BigQuery")


def _process_low_volume(**_) -> None:
    log.info("LOW VOLUME branch: выполняем лёгкую агрегацию локально")


def _on_failure_callback(context) -> None:
    """Fallback-логика + email при неуспехе пайплайна."""
    from airflow.utils.email import send_email

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context["logical_date"]
    log_url = context["task_instance"].log_url

    subject = f"[Airflow][FAILED] {dag_id}.{task_id} @ {exec_date}"
    html = f"""
        <h3>Pipeline failed</h3>
        <p><b>DAG:</b> {dag_id}</p>
        <p><b>Task:</b> {task_id}</p>
        <p><b>Execution:</b> {exec_date}</p>
        <p><a href="{log_url}">Open logs</a></p>
    """
    try:
        send_email(to=[NOTIFY_EMAIL], subject=subject, html_content=html)
    except Exception as e:  # не валим callback
        log.warning("Failed to send failure email: %s", e)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": [NOTIFY_EMAIL],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
        dag_id="marketing_batch_pipeline",
        description="POC: пакетная обработка маркетинговых данных (Task 1)",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["poc", "task-1", "marketing"],
) as dag:

    read_csv_deliveries = PythonOperator(
        task_id="read_csv_deliveries",
        python_callable=_read_csv_deliveries,
    )

    read_postgres_orders = PythonOperator(
        task_id="read_postgres_orders",
        python_callable=_read_postgres_orders,
    )

    analyze_and_branch = BranchPythonOperator(
        task_id="analyze_and_branch",
        python_callable=_analyze_and_branch,
    )

    process_high_volume = PythonOperator(
        task_id="process_high_volume",
        python_callable=_process_high_volume,
    )

    process_low_volume = PythonOperator(
        task_id="process_low_volume",
        python_callable=_process_low_volume,
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to=[NOTIFY_EMAIL],
        subject="[Airflow][OK] marketing_batch_pipeline finished",
        html_content=(
            "<h3>Pipeline finished successfully</h3>"
            "<p>DAG <b>marketing_batch_pipeline</b> completed without errors.</p>"
        ),
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    [read_csv_deliveries, read_postgres_orders] >> analyze_and_branch
    analyze_and_branch >> [process_high_volume, process_low_volume] >> send_success_email