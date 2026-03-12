import subprocess
import sys
import time
from datetime import datetime


def print_header(text):
    print("\n" + "=" * 60)
    print(f" {text}")
    print("=" * 60)


def run_command(cmd, shell=False):
    print(f"\n> {' '.join(cmd) if not shell else cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True, shell=shell)
    if result.stdout:
        print(result.stdout)
    if result.stderr and result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
    return result


def main():
    print_header("ETL PIPELINE DEMO")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    run_command([
        "kubectl", "exec", "-n", "etl-pipeline", "deployment/agent-agent", "--",
        "python", "-c",
        "from agent.agent import run_agent; import asyncio; asyncio.run(run_agent())"
    ])

    run_command([
        "python", "scripts/generate_events.py",
        "--topic", "raw_events",
        "--num", "50",
        "--rate", "5"
    ])

    print("5 sec to settle")
    time.sleep(5)

    print_header("STEP 3: Running batch ETL job")
    run_command([
        "kubectl", "exec", "-n", "etl-pipeline", "deployment/spark-master-0", "--",
        "spark-submit",
        "--master", "spark://spark-master-svc:7077",
        "--deploy-mode", "client",
        "/opt/bitnami/spark/examples/src/main/python/etl_example.py"
    ])

    print_header("STEP 4: Checking ClickHouse results")
    # result = run_command([
    #     "kubectl", "exec", "-n", "etl-pipeline", "chi-clickhouse-clickhouse-0-0-0", "--",
    #     "clickhouse-client", "--query",
    #     "SELECT count() FROM events"
    # ])

    print_header("STEP 5: Sample events")
    run_command([
        "kubectl", "exec", "-n", "etl-pipeline", "chi-clickhouse-clickhouse-0-0-0", "--",
        "clickhouse-client", "--query",
        "SELECT event_type, count() FROM events GROUP BY event_type ORDER BY count() DESC LIMIT 5"
    ])

    print_header("DEMO COMPLETE")
    print('done')


if __name__ == "__main__":
    main()