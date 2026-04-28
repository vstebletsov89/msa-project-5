#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Task 3. End-to-end demo of Kubernetes CronJob in MiniKube.
#
# Скрипт:
#   1) запускает MiniKube (если не запущен),
#   2) собирает Docker-образ экспортера в Docker-демоне MiniKube,
#   3) применяет манифесты Postgres + CronJob,
#   4) дожидается готовности Postgres,
#   5) запускает Job вручную (без ожидания 20:00),
#   6) ждёт завершения Job и показывает:
#        - логи пода (строка "Export finished: rows=5000 ..."),
#        - статусы cronjob / jobs / pods,
#        - список CSV в поде и первые строки файла.
#
# Файлы остаются внутри пода — это достаточно для демонстрации задания
# (см. скриншоты/скринкасты). Под удаляется через ttlSecondsAfterFinished=600.
#
# Требования: bash, kubectl, minikube, docker.
# -----------------------------------------------------------------------------
set -euo pipefail

NAMESPACE="shipments-export"
IMAGE_NAME="shipments-exporter"
IMAGE_TAG="v1.0.0"
JOB_NAME="shipments-exporter-manual"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

for cmd in kubectl minikube docker; do
  command -v "$cmd" >/dev/null 2>&1 || fail "'$cmd' not found in PATH"
done

# 1) MiniKube
if ! minikube status >/dev/null 2>&1; then
  log "Starting MiniKube (driver=docker)"
  minikube start --driver=docker
else
  log "MiniKube is already running"
fi

# 2) Docker image inside MiniKube's docker daemon
log "Switching docker context to MiniKube and building image ${IMAGE_NAME}:${IMAGE_TAG}"
# shellcheck disable=SC2046
eval $(minikube -p minikube docker-env --shell bash)
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" "${SCRIPT_DIR}"

# 3) Apply manifests (Postgres + CronJob)
log "Applying Kubernetes manifests"
kubectl apply -f "${SCRIPT_DIR}/k8s/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/secret.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/postgres-configmap.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/postgres-pvc.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/postgres-deployment.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/postgres-service.yaml"
kubectl apply -f "${SCRIPT_DIR}/k8s/exporter-cronjob.yaml"

# 4) Wait for Postgres
log "Waiting for Postgres to become ready (it also runs init.sql with 5000 rows)"
kubectl -n "${NAMESPACE}" rollout status deploy/postgres --timeout=180s
kubectl -n "${NAMESPACE}" wait --for=condition=ready pod -l app=postgres --timeout=180s

# 5) Run exporter Job manually (no waiting until 20:00)
log "Cleaning previous manual Job (if any) and launching a new one"
kubectl -n "${NAMESPACE}" delete job "${JOB_NAME}" --ignore-not-found=true
kubectl apply -f "${SCRIPT_DIR}/k8s/exporter-manual-job.yaml"

log "Waiting for Job ${JOB_NAME} to complete"
if ! kubectl -n "${NAMESPACE}" wait --for=condition=complete "job/${JOB_NAME}" --timeout=300s; then
  log "Job did not complete successfully — showing diagnostics"
  kubectl -n "${NAMESPACE}" describe "job/${JOB_NAME}" || true
  kubectl -n "${NAMESPACE}" logs -l job-name="${JOB_NAME}" --tail=200 || true
  fail "Manual Job failed"
fi

# 6) Show demo artifacts via kubectl only (no mounts, no cp)
log "Job logs (stdout of the exporter):"
kubectl -n "${NAMESPACE}" logs -l job-name="${JOB_NAME}" --tail=200

log "Cluster status — CronJob / Jobs / Pods:"
kubectl -n "${NAMESPACE}" get cronjob,jobs,pods -o wide || true

cat <<EOF

Демонстрация завершена. Артефакты можно посмотреть командами ниже
(под сохраняется ~10 минут благодаря ttlSecondsAfterFinished=600):

  # Посмотреть расписание CronJob:
  kubectl -n ${NAMESPACE} get cronjob shipments-exporter

  # Логи завершённого Job:
  kubectl -n ${NAMESPACE} logs -l job-name=${JOB_NAME}

  # Описание Job (статус, события, retries):
  kubectl -n ${NAMESPACE} describe job/${JOB_NAME}

  # Полная очистка:
  kubectl delete namespace ${NAMESPACE}
EOF