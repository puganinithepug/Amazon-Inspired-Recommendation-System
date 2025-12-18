
FROM python:3.11-slim
WORKDIR /app

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY monitoring/requirements-evaluator.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scripts/online_evaluation.py /app/scripts/online_evaluation.py

ENV KAFKA_BROKER=fall2025-comp585.cs.mcgill.ca:9092
ENV RECOMMENDATION_LOG_FILE=/recommendation-logs/recommendations.log
VOLUME ["/metrics-data"]
VOLUME ["/recommendation-logs"]

# run every 5 minutes; write JSON into /metrics-data (so exporter can read it)
CMD ["bash", "-c", "while true; do cd /metrics-data && python -u /app/scripts/online_evaluation.py || true; sleep 300; done"]
