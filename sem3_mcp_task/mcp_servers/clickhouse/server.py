import hashlib
import json
import time

from fastmcp import FastMCP
from clickhouse_driver import Client
from mcp_servers.common.idempotency import IdempotencyKey, RequestDeduplicator
import os
from mcp_servers.common.metrics import (
    track_request, MetricsMiddleware,
    CLICKHOUSE_QUERY_DURATION, CLICKHOUSE_ROWS_WRITTEN
)

from sem3_mcp_task.mcp_servers.kafka.server import get_context

mcp = FastMCP("mcp-clickhouse")

app = mcp._app
app.add_middleware(MetricsMiddleware, server_name="clickhouse")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

_client = None

def get_client():
    global _client
    if _client is None:
        _client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            settings={"use_numpy": False}
        )
    return _client


@mcp.tool()
@track_request(server="clickhouse", tool="write")
def write(table: str, data: list, deduplication_key: str = None):
    start = time.time()
    ctx = get_context()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    idem_params = {"table": table}
    if deduplication_key:
        idem_params["deduplication_key"] = deduplication_key
    else:
        idem_params["data_hash"] = hashlib.sha256(json.dumps(data).encode()).hexdigest()[:16]

    idem_key = IdempotencyKey.generate("clickhouse_write", idem_params)

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    client = get_client()

    if deduplication_key:
        for item in data:
            item['_version'] = int(time.time())

    result = {"written": len(data)}

    IdempotencyKey.store(idem_key, result)
    RequestDeduplicator.log_request(request_id, result)

    duration = time.time() - start
    CLICKHOUSE_QUERY_DURATION.labels(operation="insert").observe(duration)
    CLICKHOUSE_ROWS_WRITTEN.labels(table=table).inc(len(data))
    return result


@mcp.tool()
@track_request(server="clickhouse", tool="query")
def query(sql: str, limit: int = 100):
    start = time.time()
    client = get_client()

    result = client.execute(sql)

    duration = time.time() - start
    CLICKHOUSE_QUERY_DURATION.labels(operation="select").observe(duration)

    return {"rows": result[:limit], "row_count": len(result)}


@mcp.tool()
def create_table(name: str, schema: str, engine: str = "MergeTree()", order_by: str = None):
    ctx = get_context()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    idem_key = IdempotencyKey.generate("create_table", {
        "name": name,
        "schema": schema,
        "engine": engine,
        "order_by": order_by
    })

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    client = get_client()
    query = f"CREATE TABLE IF NOT EXISTS {name} ({schema}) ENGINE = {engine}"
    if order_by:
        query += f" ORDER BY {order_by}"
    client.execute(query)

    result = {"table": name, "created": True}

    IdempotencyKey.store(idem_key, result)
    RequestDeduplicator.log_request(request_id, result)
    return result