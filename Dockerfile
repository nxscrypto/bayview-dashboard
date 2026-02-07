FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Force Docker to not cache from here
ARG CACHEBUST=1

ENV REFRESH_MINUTES=15

CMD ["sh", "-c", "echo Starting gunicorn on port $PORT && exec gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 120 --access-logfile -"]
