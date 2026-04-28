"""
Shipments exporter for Kubernetes CronJob (Task 3).

Читает таблицу `shipments` из PostgreSQL и выгружает её в CSV-файл
в локальную директорию пода (см. EXPORT_DIR).

Имя файла: <table>_<YYYYMMDD_HHMMSS>.csv

Параметры берутся из переменных окружения:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    EXPORT_TABLE  (default: shipments),
    EXPORT_DIR    (default: /exports).

Exit code:
    0 — успех,
    !=0 — ошибка (Kubernetes перезапустит Job согласно backoffLimit).
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
log = logging.getLogger("shipments-exporter")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def export_shipments() -> None:
    table = os.environ.get("EXPORT_TABLE", "shipments")
    export_dir = Path(os.environ.get("EXPORT_DIR", "/exports"))
    export_dir.mkdir(parents=True, exist_ok=True)

    pg_dsn = (
        f"host={_require_env('POSTGRES_HOST')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={_require_env('POSTGRES_DB')} "
        f"user={_require_env('POSTGRES_USER')} "
        f"password={_require_env('POSTGRES_PASSWORD')}"
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = export_dir / f"{table}_{timestamp}.csv"

    log.info("Starting export of table '%s' to %s", table, output_file)

    rows_total = 0
    with psycopg.connect(pg_dsn) as conn:
        # server-side cursor — безопасно для таблиц на 200k+ строк
        with conn.cursor(name="shipments_export_cursor") as cur:
            cur.itersize = 5_000
            cur.execute(f'SELECT * FROM "{table}"')  # noqa: S608 — table из env, контролируется оператором
            columns = [desc.name for desc in cur.description]

            with output_file.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(columns)
                for row in cur:
                    writer.writerow(row)
                    rows_total += 1

    size_bytes = output_file.stat().st_size
    log.info(
        "Export finished: rows=%d, bytes=%d, file=%s",
        rows_total,
        size_bytes,
        output_file,
    )


def main() -> int:
    try:
        export_shipments()
        return 0
    except Exception:
        log.exception("Export failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())