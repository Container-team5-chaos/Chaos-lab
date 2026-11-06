# 빌드 위치: 레포 루트 (experiments/를 같이 넣기 위해)
#   docker build -f orchestrator/Dockerfile -t chaos-orchestrator:dev .
FROM python:3.11-slim

WORKDIR /app
COPY orchestrator/pyproject.toml orchestrator/README.md /app/orchestrator/
COPY orchestrator/src /app/orchestrator/src
RUN pip install --no-cache-dir "/app/orchestrator[k8s]"

COPY experiments /app/experiments

ENV ORCH_STATE_PATH=/var/lib/orchestrator/state.json \
    PYTHONUNBUFFERED=1
WORKDIR /var/lib/orchestrator

# tini 없이도 SIGTERM이 python(PID 1)으로 바로 간다. main.py가 받아서 정리 후 ABORTED를 남긴다.
ENTRYPOINT ["orchestrator"]
