import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from agent.schema_validator import SchemaValidator
from mcp_servers.common.idempotency import IdempotencyKey
from mcp_servers.common.metrics import REQUEST_COUNT

def test_schema_validation():
    schema = {
        "type": "record",
        "name": "Test",
        "fields": [{"name": "id", "type": "int"}]
    }
    assert SchemaValidator.validate_avro_schema(schema) == True

def test_idempotency():
    key1 = IdempotencyKey.generate("test", {"a": 1})
    key2 = IdempotencyKey.generate("test", {"a": 1})
    assert key1 == key2