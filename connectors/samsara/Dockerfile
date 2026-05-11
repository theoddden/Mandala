FROM python:3.11-alpine

WORKDIR /app

RUN apk add --no-cache curl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector.py .

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

EXPOSE 9000

CMD ["uvicorn", "connector:app", "--host", "0.0.0.0", "--port", "9000"]
