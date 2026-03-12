import os
import json
import re
import sys
import requests
from dotenv import load_dotenv

env_path = '/app/.env'
if os.path.exists(env_path):
    load_dotenv(env_path)

LLM_URL = "http://172.30.112.1:1234/v1/chat/completions"
MODEL_NAME = os.getenv("MODEL_NAME", "phi-3.1-mini-4k-instruct")
PLAN_SYSTEM = """You are an ETL assistant. Return ONLY valid JSON.
IMPORTANT: No trailing commas in arrays or objects.

Schemas must be validated for compatibility. Available schemas:
- raw_events: {event_id: string, timestamp: long, event_type: string, user_id: int, value: double, metadata: string}

When creating topics, consider the schema that will be used.
When submitting ETL jobs, ensure the output schema matches the target table.

Example with NO trailing commas:
{
  "clarify": [],
  "actions": [
    {
      "tool": "kafka_list_topics",
      "args": {}
    }
  ],
  "notes": ""
}

Available tools:
- kafka_create_topic(topic, partitions, replication, configs)
- kafka_set_retention(topic, retention_ms)
- kafka_list_acls(topic)
- kafka_list_topics
- kafka_produce_sample(topic, message)
- kafka_consume_sample(topic, timeout)
- spark_submit_job(job_type, parameters, main_class, jar_path)
- spark_job_status(submission_id)
- spark_kill_job(submission_id)
- spark_job_logs(submission_id, lines)
- clickhouse_write(table, data, deduplication_key)

Return ONLY the JSON. No other text."""


def make_plan(history):
    messages = [{"role": "system", "content": PLAN_SYSTEM}] + history
    payload = {
        "model": MODEL_NAME,
        "messages": messages,
        "max_tokens": 300,
        "temperature": 0.1
    }

    try:
        response = requests.post(LLM_URL, json=payload, timeout=(30, 300))
        response.raise_for_status()
        response_json = response.json()
        if "choices" not in response_json or len(response_json["choices"]) == 0:
            raise ValueError("No choices in LLM response")

        content = response_json["choices"][0]["message"]["content"]

        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
        content = re.sub(r'^```json\s*', '', content)
        content = re.sub(r'\s*```$', '', content)
        content = content.strip()

        json_match = re.search(r'({.*})', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)

        try:
            result = json.loads(content)
            return result
        except json.JSONDecodeError as e:
            print(e)
            raise

    except requests.exceptions.Timeout as e:
        return fallback_plan(history)
    except requests.exceptions.ConnectionError as e:
        return fallback_plan(history)
    except Exception as e:
        import traceback
        traceback.print_exc(file=sys.stderr)
        return fallback_plan(history)


def fallback_plan(history):
    if history and "topic" in history[-1]["content"].lower():
        if "create" in history[-1]["content"].lower():
            return {
                "clarify": [],
                "actions": [{
                    "tool": "kafka_create_topic",
                    "args": {
                        "topic": "raw_events",
                        "partitions": 3
                    }
                }],
                "notes": "Created topic based on fallback"
            }
        elif "list" in history[-1]["content"].lower():
            return {
                "clarify": [],
                "actions": [{
                    "tool": "kafka_list_topics",
                    "args": {}
                }],
                "notes": "Listing topics based on fallback"
            }

    return {
        "clarify": [{"question": "Could you clarify your request?"}],
        "actions": [],
        "notes": ""
    }