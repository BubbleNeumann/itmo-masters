FROM python:3.10-slim
WORKDIR /app
COPY mcp_servers/kafka/requirements.txt .
RUN pip install -r requirements.txt
COPY mcp_servers/common/ ./mcp_servers/common/
COPY mcp_servers/kafka/ ./mcp_servers/kafka/
ENV PYTHONPATH=/app
CMD ["python", "-m", "mcp_servers.kafka.server"]