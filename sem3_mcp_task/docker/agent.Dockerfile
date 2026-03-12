FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    echo "Requirements installed successfully"

COPY agent/ ./agent/
COPY mcp_servers/common/ ./mcp_servers/common/
COPY .env .env

ENV PYTHONPATH=/app
RUN python -c "print('Image build successful')"
CMD ["python", "-m", "agent.agent"]