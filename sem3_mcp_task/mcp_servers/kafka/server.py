import os

from fastmcp import FastMCP
from mcp_servers.common.audit import audit_log, AuditRecord, now_iso
from mcp_servers.common.config import get_env_str
from mcp_servers.common.idempotency import IdempotencyKey, RequestDeduplicator
from mcp_servers.common.metrics import (
    track_request, MetricsMiddleware,
    KAFKA_TOPIC_PARTITIONS, KAFKA_MESSAGES_PRODUCED, KAFKA_MESSAGES_CONSUMED
)

from . import kafka_client

mcp = FastMCP("mcp-kafka")

# metrics middleware
app = mcp._app
app.add_middleware(MetricsMiddleware, server_name="kafka")

USER_ID = get_env_str("MCP_USER_ID", "local-user")
SOURCE = get_env_str("MCP_SOURCE", "kafka-server")

def get_context():
    return {
        "user_id": os.environ.get("MCP_USER_ID", USER_ID),
        "request_id": os.environ.get("MCP_REQUEST_ID", "req-" + now_iso()),
        "source": SOURCE,
    }


@mcp.tool()
@track_request(server="kafka", tool="create_topic")
def create_topic(topic: str, partitions: int = 1, replication: int = 1, configs: dict = None):
    ctx = get_context()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    idem_key = IdempotencyKey.generate("create_topic", {
        "topic": topic,
        "partitions": partitions,
        "replication": replication,
        "configs": configs
    })

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="create_topic",
                      args={"topic": topic, "partitions": partitions, "replication": replication, "configs": configs},
                      status="STARTED")
    audit_log(rec)
    #topic metrics
    KAFKA_TOPIC_PARTITIONS.labels(topic=topic).set(partitions)
    try:
        result = kafka_client.create_topic(topic, partitions, replication, configs)

        # idempotency res
        IdempotencyKey.store(idem_key, result)
        RequestDeduplicator.log_request(request_id, result)

        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise


@mcp.tool()
def set_retention(topic: str, retention_ms: int):
    ctx = get_context()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    idem_key = IdempotencyKey.generate("set_retention", {
        "topic": topic,
        "retention_ms": retention_ms
    })

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="set_retention", args={"topic": topic, "retention_ms": retention_ms}, status="STARTED")
    audit_log(rec)

    try:
        result = kafka_client.set_retention(topic, retention_ms)

        IdempotencyKey.store(idem_key, result)
        RequestDeduplicator.log_request(request_id, result)

        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
def list_acls(topic: str):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="list_acls", args={"topic": topic}, status="STARTED")
    audit_log(rec)
    result = kafka_client.list_acls(topic)
    audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
    return result

@mcp.tool()
@track_request(server="kafka", tool="list_topics")
def list_topics():
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="list_topics", args={}, status="STARTED")
    audit_log(rec)
    try:
        result = kafka_client.list_topics()
        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
@track_request(server="kafka", tool="produce_sample")
def produce_sample(topic: str, message: str):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="produce_sample", args={"topic": topic, "message": message}, status="STARTED")
    audit_log(rec)
    try:
        result = kafka_client.produce_sample(topic, message)
        KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()
        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
@track_request(server="kafka", tool="consume_sample")
def consume_sample(topic: str, timeout: float = 5.0):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="consume_sample", args={"topic": topic, "timeout": timeout}, status="STARTED")
    audit_log(rec)
    try:
        result = kafka_client.consume_sample(topic, timeout)
        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": {"count": len(result["messages"])}}))
        message_count = len(result.get('messages', []))
        KAFKA_MESSAGES_CONSUMED.labels(topic=topic).inc(message_count)
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
def reassign_partitions(topic: str, reassignment: dict):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="reassign_partitions", args={"topic": topic, "reassignment": reassignment}, status="STARTED")
    audit_log(rec)
    result = kafka_client.reassign_partitions(topic, reassignment)
    audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
    return result

@mcp.tool()
def compact_now(topic: str):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="compact_now", args={"topic": topic}, status="STARTED")
    audit_log(rec)
    result = kafka_client.compact_now(topic)
    audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
    return result


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)