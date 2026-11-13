FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --create-home adapter
COPY adapter ./adapter
USER 10001
EXPOSE 8000
CMD ["python", "-m", "adapter"]
