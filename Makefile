.DEFAULT_GOAL := help

.PHONY: help install lint format typecheck test check contracts ingest features train serve benchmark benchmark-resilience tools helm-lint cluster cluster-test cluster-status airflow-password airflow-ui mlflow-ui serving-deploy serving-ui cluster-delete

PYTHON ?= python3.12

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install the package and development dependencies
	$(PYTHON) -m pip install -e '.[dev]'

lint: ## Run static lint checks
	bash -n scripts/*.sh
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format: ## Apply automatic formatting and safe lint fixes
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

typecheck: ## Run strict static type checking
	$(PYTHON) -m mypy src

test: ## Run the unit test suite with coverage
	$(PYTHON) -m pytest

check: lint typecheck test helm-lint ## Run all local quality gates

contracts: ## Export event contracts as JSON Schema
	$(PYTHON) -m tripml contracts export --output build/contracts

ingest: ## Ingest a TLC month (MONTH=YYYY-MM)
	@test -n "$(MONTH)" || (echo "MONTH is required (example: make ingest MONTH=2024-01)" && exit 2)
	$(PYTHON) -m tripml ingest --month "$(MONTH)"

features: ## Build point-in-time gold features (MONTH=YYYY-MM)
	@test -n "$(MONTH)" || (echo "MONTH is required (example: make features MONTH=2024-01)" && exit 2)
	$(PYTHON) -m tripml features build --month "$(MONTH)"

train: ## Train, compare, and promotion-gate model candidates
	$(PYTHON) -m tripml train

serve: ## Serve production ETA predictions with optional Redis features
	$(PYTHON) -m tripml serve

benchmark: ## Benchmark serving (BENCHMARK_ARGS='--output artifacts/benchmarks/run ...')
	$(PYTHON) -m tripml benchmark --requests-file examples/benchmark/requests.jsonl $(BENCHMARK_ARGS)

benchmark-resilience: ## Test isolated serving failures and recovery (OUTPUT=new/evidence/path)
	@test -n "$(OUTPUT)" || (echo "OUTPUT must name a new evidence directory" && exit 2)
	TRIPML_TEST_SERVING_RESILIENCE=1 TRIPML_RESILIENCE_OUTPUT="$(OUTPUT)" \
	  $(PYTHON) -m pytest tests/e2e/test_serving_resilience.py --no-cov

tools: ## Install pinned kind and Helm binaries into .tools/bin
	./scripts/bootstrap-tools.sh all

helm-lint: ## Lint and render the local platform chart
	./scripts/bootstrap-tools.sh helm
	./scripts/validate-chart.sh

cluster: ## Create the kind cluster and install the local infrastructure chart
	./scripts/cluster.sh create

cluster-test: ## Run the infrastructure smoke tests against the existing cluster
	./scripts/cluster.sh test

cluster-status: ## Show local cluster workloads and services
	./scripts/cluster.sh status

airflow-password: ## Print the generated local Airflow admin password
	./scripts/cluster.sh airflow-password

airflow-ui: ## Forward the local Airflow UI to http://localhost:8080
	./scripts/cluster.sh airflow-ui

mlflow-ui: ## Forward the local MLflow UI to http://localhost:5000
	./scripts/cluster.sh mlflow-ui

serving-deploy: ## Build and deploy serving after a production model exists in cluster MLflow
	./scripts/cluster.sh serving-deploy

serving-ui: ## Forward the prediction API to http://localhost:8000
	./scripts/cluster.sh serving-ui

cluster-delete: ## Delete the local kind cluster and all of its data
	./scripts/cluster.sh delete
