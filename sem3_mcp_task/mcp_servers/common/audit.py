import json
import os
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

AUDIT_LOG_PATH = os.environ.get("AUDIT_LOG_PATH", "/var/log/mcp/audit.jsonl")

@dataclass
class AuditRecord:
    ts: str
    user_id: str
    request_id: str
    source: str
    tool: str
    args: dict
    status: str
    error: str = None
    result_preview: dict = None

def audit_log(rec: AuditRecord):
    os.makedirs(os.path.dirname(AUDIT_LOG_PATH) or ".", exist_ok=True)
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")

def now_iso():
    return datetime.now(timezone.utc).isoformat()