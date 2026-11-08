FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# root가 아닌 고정 UID로 실행
RUN useradd --uid 10001 --create-home appuser
USER 10001

EXPOSE 8000
CMD ["python", "-m", "app"]
