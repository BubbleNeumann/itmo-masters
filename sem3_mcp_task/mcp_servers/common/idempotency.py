import hashlib
import json
import time
from typing import Optional, Dict, Any

_idempotency_store = {}
_request_log = {}


class IdempotencyKey:
    @staticmethod
    def generate(operation: str, args: Dict) -> str:
        content = f"{operation}:{json.dumps(args, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    @staticmethod
    def is_duplicate(key: str, ttl_seconds: int = 3600):
        if key in _idempotency_store:
            timestamp, result = _idempotency_store[key]
            if time.time() - timestamp < ttl_seconds:
                return True, result
        return False, None

    @staticmethod
    def store(key: str, result: Any, ttl_seconds: int = 3600):
        _idempotency_store[key] = (time.time(), result)

    @staticmethod
    def cleanup(ttl_seconds: int = 3600):
        now = time.time()
        expired = [k for k, (ts, _) in _idempotency_store.items() if now - ts > ttl_seconds]
        for k in expired:
            del _idempotency_store[k]


class RequestDeduplicator:
    @staticmethod
    def get_request_id(headers: Dict) -> Optional[str]:
        return headers.get('X-Request-ID') or headers.get('x-request-id')

    @staticmethod
    def is_duplicate_request(request_id: str):
        if request_id in _request_log:
            return True, _request_log[request_id]
        return False, None

    @staticmethod
    def log_request(request_id: str, result: Any):
        _request_log[request_id] = result

    @staticmethod
    def cleanup_requests(max_age: int = 3600):
        pass