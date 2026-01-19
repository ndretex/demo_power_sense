FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
RUN rm -f /app/app/logging.py || true

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python", "/app/app/main.py"]
