FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY exporter/ /app/exporter/

# non-root user
RUN useradd --system --uid 10001 --no-create-home appuser \
 && mkdir -p /exports \
 && chown -R 10001:10001 /exports
USER 10001

ENV EXPORT_DIR=/exports

ENTRYPOINT ["python", "/app/exporter/export.py"]