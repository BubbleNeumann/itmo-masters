FROM python:3.9-slim

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir fastapi uvicorn kafka-python prometheus-client pydantic

COPY src/mcp_kafka/ ./src/mcp_kafka/
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV KAFKA_CLIENT_ID=mcp-kafka-server
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV METRICS_PORT=8001

EXPOSE 8000 8001

CMD ["uvicorn", "src.mcp_kafka.main:app", "--host", "0.0.0.0", "--port", "8000"]