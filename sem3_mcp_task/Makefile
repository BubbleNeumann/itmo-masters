SHELL := powershell.exe
.SHELLFLAGS := -NoProfile -Command

KIND_CLUSTER ?= etl-pipeline
NAMESPACE ?= etl-pipeline
OPENAI_API_KEY ?= $(shell powershell -Command "(Read-Host 'Enter OpenAI API Key').Trim()")

load-images:
	@echo "Loading images into Kind..."
	kind load docker-image mcp-kafka:latest --name etl-pipeline
	kind load docker-image mcp-spark:latest --name etl-pipeline
	kind load docker-image mcp-clickhouse:latest --name etl-pipeline
	kind load docker-image agent-pipeline:latest --name etl-pipeline

up: kind-create local-registry build-base helmfile-apply

kind-create:
	@if (!(kind get clusters | Select-String -Quiet $(KIND_CLUSTER))) { \
		Write-Host "Creating kind cluster $(KIND_CLUSTER)..."; \
		kind create cluster --name $(KIND_CLUSTER) --config kind-config.yaml; \
	} else { \
		Write-Host "Cluster $(KIND_CLUSTER) already exists"; \
	}

# for kind
local-registry:
	@if (!(docker ps | Select-String -Quiet kind-registry)) { \
		Write-Host "Starting local registry..."; \
		docker run -d --restart=always -p 5001:5000 --name kind-registry registry:2; \
		$$containerId = docker ps -q -f name=kind-registry; \
		docker network connect kind $$containerId 2>`$$null; \
	}

# base images
build-base:
	Write-Host "Building base images..."
	docker build -f docker/agent.Dockerfile -t localhost:5001/agent:latest .
	docker build -f docker/mcp-kafka.Dockerfile -t localhost:5001/mcp-kafka:latest .
	docker build -f docker/mcp-spark.Dockerfile -t localhost:5001/mcp-spark:latest .
	docker build -f docker/clickhouse.Dockerfile -t localhost:5001/mcp-clickhouse:latest .
	docker push localhost:5001/agent:latest
	docker push localhost:5001/mcp-kafka:latest
	docker push localhost:5001/mcp-spark:latest
	docker push localhost:5001/mcp-clickhouse:latest

helmfile-apply:
	$$env:OPENAI_API_KEY="$(OPENAI_API_KEY)"; helmfile sync

# for local access
port-forward:
	Write-Host "Port-forwarding services..."
	Start-Job -ScriptBlock { kubectl port-forward -n $(NAMESPACE) svc/kafka 9092:9092 }
	Start-Job -ScriptBlock { kubectl port-forward -n $(NAMESPACE) svc/clickhouse 8123:8123 9000:9000 }
	Start-Job -ScriptBlock { kubectl port-forward -n $(NAMESPACE) svc/spark-master 8080:8080 }
	Start-Job -ScriptBlock { kubectl port-forward -n $(NAMESPACE) svc/agent 8000:8000 }

# demo: port-forward
# 	Write-Host "Generating synthetic events..."
# 	python scripts/generate_events.py
# 	Write-Host "Attaching to agent..."
# 	kubectl exec -n $(NAMESPACE) -it deployment/agent-pipeline-agent -- python -m agent.agent

demo:
	@kubectl exec -n etl-pipeline -it deployment/agent-agent -- python -c "from agent.agent import run_agent; import asyncio;asyncio.run(run_agent())"
	@python scripts/generate_events.py --topic raw_events --num 20 --rate 2
	@echo "complete"

# clean up
down: helmfile-destroy kind-delete

helmfile-destroy:
	helmfile destroy

kind-delete:
	Write-Host "Deleting kind cluster..."
	kind delete cluster --name $(KIND_CLUSTER)
	Write-Host "Stopping local registry..."
	docker stop kind-registry 2>`$$null
	docker rm kind-registry 2>`$$null

# interactive agent session
agent:
	kubectl exec -n $(NAMESPACE) -it deployment/agent-pipeline-agent -- python -m agent.agent

logs:
	kubectl logs -n $(NAMESPACE) -l app=agent-pipeline

status:
	kubectl get all -n $(NAMESPACE)

init-clickhouse:
	@type jobs\init_clickhouse.sql | kubectl exec -n etl-pipeline chi-clickhouse-clickhouse-0-0-0 -- clickhouse-client --multiquery

generate-events:
	python scripts/generate_events.py --topic raw_events --num 100 --rate 10

generate-load:
	python scripts/generate_events.py --topic raw_events --num 1000 --rate 50

restart-mcp:
	@echo "Restarting MCP deployments..."
	kubectl rollout restart deployment -n etl-pipeline mcp-kafka-mcp-kafka
	kubectl rollout restart deployment -n etl-pipeline mcp-spark-mcp-spark
	kubectl rollout restart deployment -n etl-pipeline mcp-clickhouse-mcp-clickhouse
	kubectl rollout restart deployment -n etl-pipeline agent-agent
	kubectl get pods -n etl-pipeline -w

test:
	@python tests/run.py