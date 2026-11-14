# 빌드 위치: 레포 루트 (experiments/를 같이 넣기 위해)
#   docker build -f orchestrator/Dockerfile -t chaos-orchestrator:dev .
FROM python:3.11-slim

WORKDIR /app
COPY orchestrator/pyproject.toml orchestrator/README.md /app/orchestrator/
COPY orchestrator/src /app/orchestrator/src
RUN pip install --no-cache-dir "/app/orchestrator[k8s]"

COPY experiments /app/experiments
COPY load/k6 /app/load/k6

# k6 Job 시작(k6load)과 judge 증거 수집이 kubectl 명령을 부른다
COPY --from=registry.k8s.io/kubectl:v1.35.3 /bin/kubectl /usr/local/bin/kubectl

ENV ORCH_STATE_PATH=/var/lib/orchestrator/state.json \
    K6_DIR=/app/load/k6 \
    PYTHONUNBUFFERED=1
WORKDIR /var/lib/orchestrator

# tini 없이도 SIGTERM이 python(PID 1)으로 바로 간다. main.py가 받아서 정리 후 ABORTED를 남긴다.
ENTRYPOINT ["orchestrator"]
