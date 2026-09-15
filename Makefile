.DEFAULT_GOAL := help

.PHONY: help install lint format typecheck test check contracts

PYTHON ?= python3.12

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install the package and development dependencies
	$(PYTHON) -m pip install -e '.[dev]'

lint: ## Run static lint checks
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format: ## Apply automatic formatting and safe lint fixes
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

typecheck: ## Run strict static type checking
	$(PYTHON) -m mypy src

test: ## Run the unit test suite with coverage
	$(PYTHON) -m pytest

check: lint typecheck test ## Run all local quality gates

contracts: ## Export event contracts as JSON Schema
	$(PYTHON) -m tripml contracts export --output build/contracts
