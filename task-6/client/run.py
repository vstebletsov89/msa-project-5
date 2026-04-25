"""
Раз в N секунд дёргает endpoint запуска Spring Batch ETL Job.
Для каждого вызова генерирует свой B3 traceId — он же будет виден
в логах сервера в Kibana.

"""

import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

URL = "http://localhost:8080/api/jobs/import-products"
RUNS = 3
SLEEP_SECONDS = 60


def gen_b3_headers() -> dict:
    return {
        "X-B3-TraceId": uuid.uuid4().hex,        # 128-bit trace id
        "X-B3-SpanId":  uuid.uuid4().hex[:16],   # 64-bit span id
        "X-B3-Sampled": "1",
    }


def call(file_name: str, headers: dict) -> tuple[int, str]:
    qs = urllib.parse.urlencode({"fileName": file_name})
    req = urllib.request.Request(
        url=f"{URL}?{qs}",
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def main() -> int:
    for i in range(1, RUNS + 1):
        headers = gen_b3_headers()
        try:
            status, body = call("product-data.csv", headers)
            print(
                f"#{i} status={status} "
                f"traceId={headers['X-B3-TraceId']} body={body}"
            )
        except urllib.error.URLError as e:
            print(f"#{i} ERROR traceId={headers['X-B3-TraceId']} {e}")
            return 1
        except Exception as e:
            print(f"#{i} UNEXPECTED traceId={headers['X-B3-TraceId']} {e}")
            return 1
        time.sleep(SLEEP_SECONDS)
    return 0


if __name__ == "__main__":
    sys.exit(main())