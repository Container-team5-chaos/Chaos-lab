FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# root가 아닌 사용자로 실행
RUN useradd --create-home appuser
USER appuser

EXPOSE 8000
CMD ["python", "-m", "app"]
