.PHONY: up down demo test load-test

up:
    @echo "start kuber cluster"
    kind create cluster --config kind-config.yaml || true
    @echo "installing helm charts"
    helmfile sync

down:
    helmfile destroy
    kind delete cluster

demo:
    @echo "Generating synthetic events..."
    python scripts/generate_test_data.py
    @echo "Running pipeline..."
    curl -X POST http://localhost:8000/api/request \
      -d '{"request": "Create topic raw_events with 7-day retention, ETL to ClickHouse"}'
    @echo "Rendering report..."
    python scripts/render_report.py

test:
    pytest tests/unit
    pytest tests/integration

load-test:
    python scripts/run_load_test.py --throughput 100MB/min
