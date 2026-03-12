import uuid
import json

def new_request_id() -> str:
    return f"agent-{uuid.uuid4().hex[:10]}"

def json_dumps_safe(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)

def truncate(s: str, n: int = 240) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n-3] + "..."