import functools
import time

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CollectorRegistry

REGISTRY = CollectorRegistry(auto_describe=True)

REQUEST_COUNT = Counter(
    'mcp_requests_total',
    'Total MCP requests',
    ['server', 'tool', 'status'],
    registry=REGISTRY
)

REQUEST_DURATION = Histogram(
    'mcp_request_duration_seconds',
    'MCP request duration in seconds',
    ['server', 'tool'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
    registry=REGISTRY
)

KAFKA_TOPIC_PARTITIONS = Gauge(
    'kafka_topic_partitions',
    'Number of partitions per topic',
    ['topic'],
    registry=REGISTRY
)

KAFKA_MESSAGES_PRODUCED = Counter(
    'kafka_messages_produced_total',
    'Total messages produced to Kafka',
    ['topic'],
    registry=REGISTRY
)

KAFKA_MESSAGES_CONSUMED = Counter(
    'kafka_messages_consumed_total',
    'Total messages consumed from Kafka',
    ['topic'],
    registry=REGISTRY
)

SPARK_JOBS_SUBMITTED = Counter(
    'spark_jobs_submitted_total',
    'Total Spark jobs submitted',
    ['job_type', 'status'],
    registry=REGISTRY
)

SPARK_JOB_DURATION = Histogram(
    'spark_job_duration_seconds',
    'Spark job duration in seconds',
    ['job_type'],
    buckets=(5, 10, 30, 60, 120, 300, 600, 1800, 3600),
    registry=REGISTRY
)

SPARK_JOB_ACTIVE = Gauge(
    'spark_jobs_active',
    'Number of active Spark jobs',
    ['job_type'],
    registry=REGISTRY
)

CLICKHOUSE_QUERY_DURATION = Histogram(
    'clickhouse_query_duration_seconds',
    'ClickHouse query duration',
    ['operation'],
    registry=REGISTRY
)

CLICKHOUSE_ROWS_WRITTEN = Counter(
    'clickhouse_rows_written_total',
    'Total rows written to ClickHouse',
    ['table'],
    registry=REGISTRY
)

# correlation, job - topic
JOB_TOPIC_RELATION = Gauge(
    'job_topic_relation',
    'Relationship between Spark jobs and Kafka topics',
    ['job_id', 'job_type', 'topic', 'operation'],
    registry=REGISTRY
)


def track_request(server: str, tool: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            status = "success"
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                status = "error"
                raise
            finally:
                duration = time.time() - start
                REQUEST_COUNT.labels(server=server, tool=tool, status=status).inc()
                REQUEST_DURATION.labels(server=server, tool=tool).observe(duration)

        return wrapper

    return decorator


class MetricsMiddleware:

    def __init__(self, app, server_name: str):
        self.app = app
        self.server_name = server_name

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/metrics":
            # serve metrics endpoint
            from starlette.responses import Response
            return Response(
                content=generate_latest(REGISTRY),
                media_type="text/plain"
            )

        start = time.time()
        status = 200

        async def wrapped_send(message):
            if message["type"] == "http.response.start":
                nonlocal status
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        finally:
            duration = time.time() - start
            REQUEST_COUNT.labels(
                server=self.server_name,
                tool="http",
                status=str(status)[0] + "xx"
            ).inc()
            REQUEST_DURATION.labels(
                server=self.server_name,
                tool="http"
            ).observe(duration)