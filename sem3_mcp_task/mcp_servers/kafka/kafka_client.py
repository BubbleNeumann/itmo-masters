import os
import subprocess
import tempfile

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "user1")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")


def list_topics():
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.properties', delete=False) as f:
            f.write(f"""security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username='{KAFKA_USERNAME}' password='{KAFKA_PASSWORD}';
""")
            props_file = f.name

        cmd = [
            "kafka-topics.sh",
            "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS,
            "--list",
            "--command-config", props_file
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        os.unlink(props_file)

        if result.returncode != 0:
            raise Exception(f"Kafka CLI error: {result.stderr}")

        topics = result.stdout.strip().split('\n')
        return {"topics": topics}

    except Exception as e:
        raise Exception(f"Kafka connection failed: {e}")


def create_topic(topic: str, partitions: int = 1, replication: int = 1, config: dict = None):
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.properties', delete=False) as f:
            f.write(f"""security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username='{KAFKA_USERNAME}' password='{KAFKA_PASSWORD}';
""")
            props_file = f.name

        cmd = [
            "kafka-topics.sh",
            "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS,
            "--create",
            "--topic", topic,
            "--partitions", str(partitions),
            "--replication-factor", str(replication),
            "--command-config", props_file,
            "--if-not-exists"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        os.unlink(props_file)

        if result.returncode != 0:
            raise Exception(f"Kafka CLI error: {result.stderr}")

        return {"status": "created", "topic": topic}

    except Exception as e:
        if "already exists" in str(e):
            return {"status": "already_exists", "topic": topic}
        raise Exception(f"Kafka connection failed: {e}")