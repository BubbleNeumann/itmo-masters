from typing import List, Dict, Tuple

from .schema_validator import SchemaValidator


def validate_schema(topic: str, schema: Dict) -> Tuple[bool, str]:
    try:
        SchemaValidator.validate_avro_schema(schema)

        existing = SchemaValidator.get_latest_schema(topic)
        if existing:
            compatible, msg = SchemaValidator.check_backward_compatible(existing, schema)
            if not compatible:
                return False, f"Schema incompatible: {msg}"

        SchemaValidator.register_schema(topic, schema)
        return True, "Schema valid and registered"
    except Exception as e:
        return False, str(e)


def validate_topic_schema(topic: str, data_sample: Dict) -> Tuple[bool, str]:
    schema = SchemaValidator.get_latest_schema(topic)
    if not schema:
        return False, f"No schema registered for topic {topic}"

    return SchemaValidator.validate_data(data_sample, schema)


def validate_retention(topic: str, retention_ms: int) -> Tuple[bool, str]:
    if retention_ms < 3600000:  # 1h
        return False, "Retention too short (minimum 1 hour)"
    if retention_ms > 2592000000:  # 30 days
        return False, "Retention too long (maximum 30 days)"
    return True, "Retention valid"


def validate_partitioning(topic: str, partitions: int) -> Tuple[bool, str]:
    if partitions < 1:
        return False, "Partitions must be at least 1"
    if partitions > 100:
        return False, "Too many partitions (max 100)"
    return True, "Partition count valid"


def check_prerequisites(plan: Dict) -> List[str]:
    errors = []

    actions = plan.get("actions", [])
    for action in actions:
        tool = action.get("tool")
        args = action.get("args", {})

        if tool == "kafka_create_topic":
            topic = args.get("topic")
            partitions = args.get("partitions", 1)

            # Validate topic name
            if not topic or not isinstance(topic, str):
                errors.append("Topic name must be a string")

            # Validate partitions
            valid, msg = validate_partitioning(topic, partitions)
            if not valid:
                errors.append(f"Invalid partition count: {msg}")

        elif tool == "kafka_set_retention":
            topic = args.get("topic")
            retention = args.get("retention_ms")

            if retention:
                valid, msg = validate_retention(topic, retention)
                if not valid:
                    errors.append(f"Invalid retention: {msg}")

        elif tool in ["spark_submit_job", "spark_job_status", "spark_kill_job", "spark_job_logs"]:
            submission_id = args.get("submission_id")
            if submission_id and not submission_id.startswith("spark-"):
                errors.append("Invalid submission ID format")

    return errors