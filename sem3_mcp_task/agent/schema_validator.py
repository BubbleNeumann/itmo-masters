import json
import os
from typing import Dict, Any, Optional, Tuple
import fastavro
from fastavro import parse_schema, validate

_schema_registry = {}


class SchemaValidator:
    @staticmethod
    def parse_schema(schema_str: str, schema_type: str = "avro") -> Dict:
        try:
            if schema_type == "avro":
                if schema_str.strip().startswith('{'):
                    return json.loads(schema_str)
                return {"type": "record", "name": "default", "fields": json.loads(schema_str)}
            elif schema_type == "json":
                return json.loads(schema_str)
            else:
                raise ValueError(f"Unsupported schema type: {schema_type}")
        except Exception as e:
            raise ValueError(f"Failed to parse schema: {e}")

    @staticmethod
    def validate_avro_schema(schema: Dict) -> bool:
        try:
            parse_schema(schema)
            return True
        except Exception as e:
            raise ValueError(f"Invalid Avro schema: {e}")

    @staticmethod
    def check_backward_compatible(old_schema: Dict, new_schema: Dict) -> Tuple[bool, str]:
        try:
            old_parsed = parse_schema(old_schema)
            new_parsed = parse_schema(new_schema)

            old_fields = {f['name']: f for f in old_parsed.get('fields', [])}
            new_fields = {f['name']: f for f in new_parsed.get('fields', [])}

            removed = set(old_fields.keys()) - set(new_fields.keys())
            if removed:
                return False, f"Fields removed: {removed}"

            # fields with changed type?
            for field_name in old_fields:
                if field_name in new_fields:
                    old_type = old_fields[field_name].get('type')
                    new_type = new_fields[field_name].get('type')
                    if old_type != new_type and not (isinstance(old_type, list) and 'null' in old_type):
                        return False, f"Field '{field_name}' type changed from {old_type} to {new_type}"

            return True, "Backward compatible"

        except Exception as e:
            return False, f"Compatibility check failed: {e}"

    @staticmethod
    def register_schema(topic: str, schema: Dict, version: int = 1) -> None:
        if topic not in _schema_registry:
            _schema_registry[topic] = {}
        _schema_registry[topic][version] = schema

    @staticmethod
    def get_latest_schema(topic: str) -> Optional[Dict]:
        if topic in _schema_registry and _schema_registry[topic]:
            latest_version = max(_schema_registry[topic].keys())
            return _schema_registry[topic][latest_version]
        return None

    @staticmethod
    def validate_data(data: Dict, schema: Dict) -> Tuple[bool, str]:
        try:
            # for avro
            if 'fields' in schema or schema.get('type') == 'record':
                is_valid = validate(data, schema, raise_on_errors=False)
                if not is_valid:
                    errors = []
                    for field in schema.get('fields', []):
                        field_name = field['name']
                        if field_name not in data:
                            errors.append(f"Missing field: {field_name}")
                    if errors:
                        return False, "; ".join(errors)
                return True, "Valid"
            else:
                for key, expected_type in schema.items():
                    if key in data:
                        actual_type = type(data[key]).__name__
                        if actual_type != expected_type:
                            return False, f"Field '{key}' expected {expected_type}, got {actual_type}"
                return True, "Valid"
        except Exception as e:
            return False, f"Validation error: {e}"


EVENT_SCHEMA = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "event_type", "type": "string"},
        {"name": "user_id", "type": "int"},
        {"name": "value", "type": "double"},
        {"name": "metadata", "type": ["null", "string"], "default": None}
    ]
}

SchemaValidator.register_schema("raw_events", EVENT_SCHEMA)