FROM python:3.11-slim
WORKDIR /app
COPY monitoring/requirements-exporter.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY scripts/metrics_exporter.py /app/scripts/metrics_exporter.py
EXPOSE 9108
CMD ["python", "/app/scripts/metrics_exporter.py"]