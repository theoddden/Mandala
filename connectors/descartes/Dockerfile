FROM python:3.11-alpine

WORKDIR /app

# Minimal deps: alpine for small footprint
RUN apk add --no-cache curl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector.py .

# Healthcheck verifies the connector is listening
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9001/health || exit 1

EXPOSE 9001

CMD ["uvicorn", "connector:app", "--host", "0.0.0.0", "--port", "9001"]
