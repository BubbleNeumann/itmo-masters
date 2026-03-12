import os
import time

from fastmcp import FastMCP
from mcp_servers.common.audit import audit_log, AuditRecord, now_iso
from mcp_servers.common.config import get_env_str
from mcp_servers.common.idempotency import IdempotencyKey, RequestDeduplicator
from mcp_servers.common.metrics import (
    track_request, MetricsMiddleware,
    SPARK_JOBS_SUBMITTED, SPARK_JOB_DURATION, SPARK_JOB_ACTIVE
)

from . import spark_submit, job_monitor

mcp = FastMCP("mcp-spark")

# metrics middleware
app = mcp._app
app.add_middleware(MetricsMiddleware, server_name="spark")

USER_ID = get_env_str("MCP_USER_ID", "local-user")
SOURCE = get_env_str("MCP_SOURCE", "spark-server")

def get_context():
    return {
        "user_id": os.environ.get("MCP_USER_ID", USER_ID),
        "request_id": os.environ.get("MCP_REQUEST_ID", "req-" + now_iso()),
        "source": SOURCE,
    }


@mcp.tool()
@track_request(server="spark", tool="submit_job")
def submit_job(job_type: str, parameters: dict, main_class: str = None, jar_path: str = None):
    ctx = get_context()
    start_time = time.time()

    SPARK_JOBS_SUBMITTED.labels(job_type=job_type, status="submitted").inc()
    SPARK_JOB_ACTIVE.labels(job_type=job_type).inc()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    # Generate idempotency key from job parameters
    idem_key = IdempotencyKey.generate(f"submit_{job_type}", {
        "job_type": job_type,
        "parameters": parameters,
        "main_class": main_class,
        "jar_path": jar_path
    })

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="submit_job", args={"job_type": job_type, "parameters": parameters, "main_class": main_class,
                                               "jar_path": jar_path},
                      status="STARTED")
    audit_log(rec)

    try:
        submission_id = spark_submit.submit_job(job_type, parameters, main_class, jar_path)
        result = {"submission_id": submission_id}

        IdempotencyKey.store(idem_key, result)
        RequestDeduplicator.log_request(request_id, result)

        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))

        duration = time.time() - start_time
        SPARK_JOB_DURATION.labels(job_type=job_type).observe(duration)
        SPARK_JOBS_SUBMITTED.labels(job_type=job_type, status="success").inc()
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise


@mcp.tool()
@track_request(server="spark", tool="job_status")
def job_status(submission_id: str):
    ctx = get_context()
    request_id = ctx["request_id"]
    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="job_status", args={"submission_id": submission_id}, status="STARTED")
    audit_log(rec)

    try:
        result = job_monitor.get_status(submission_id)

        RequestDeduplicator.log_request(request_id, result)

        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        if result.get('status') in ['COMPLETED', 'FAILED']:
            SPARK_JOB_ACTIVE.labels(job_type=result.get('job_type', 'unknown')).dec()
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise


@mcp.tool()
@track_request(server="spark", tool="kill_job")
def kill_job(submission_id: str):
    ctx = get_context()
    request_id = ctx["request_id"]

    is_duplicate, cached = RequestDeduplicator.is_duplicate_request(request_id)
    if is_duplicate:
        return cached

    idem_key = IdempotencyKey.generate("kill_job", {"submission_id": submission_id})

    is_dup, cached_result = IdempotencyKey.is_duplicate(idem_key)
    if is_dup:
        RequestDeduplicator.log_request(request_id, cached_result)
        return cached_result

    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="kill_job", args={"submission_id": submission_id}, status="STARTED")
    audit_log(rec)

    try:
        result = job_monitor.kill_job(submission_id)

        IdempotencyKey.store(idem_key, result)
        RequestDeduplicator.log_request(request_id, result)

        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
        SPARK_JOB_ACTIVE.labels(job_type='unknown').dec()
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
def job_logs(submission_id: str, lines: int = 100):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="job_logs", args={"submission_id": submission_id, "lines": lines}, status="STARTED")
    audit_log(rec)
    try:
        logs = job_monitor.get_logs(submission_id, lines)
        result = {"logs": logs}
        audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": {"length": len(logs)}}))
        return result
    except Exception as e:
        audit_log(AuditRecord(**{**rec.__dict__, "status": "ERROR", "error": str(e)}))
        raise

@mcp.tool()
def deploy_etl(helm_chart: str, values: dict):
    ctx = get_context()
    rec = AuditRecord(ts=now_iso(), user_id=ctx["user_id"], request_id=ctx["request_id"], source=ctx["source"],
                      tool="deploy_etl", args={"helm_chart": helm_chart, "values": values}, status="STARTED")
    audit_log(rec)
    # Stub: would run helm upgrade --install
    result = {"status": "deployed", "release": "etl-job"}
    audit_log(AuditRecord(**{**rec.__dict__, "status": "OK", "result_preview": result}))
    return result

if __name__ == "__main__":
    mcp.run(transport="streamable-http")