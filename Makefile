# Thin wrappers over docker compose and the local toolchain.
# Targets need a POSIX shell, so on Windows run them from Git Bash or WSL.

COMPOSE := docker compose
DBT_DIR := /app/src/dbt_project

.DEFAULT_GOAL := help
.PHONY: help up down restart ps logs build test lint format typecheck check dbt-run dbt-test clean

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start Redpanda, the producer, the streaming consumer and the dashboard
	$(COMPOSE) up -d --build

down: ## Stop the stack and remove its volumes
	$(COMPOSE) down -v

restart: down up ## Recreate the stack from scratch

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow logs for every service
	$(COMPOSE) logs -f --tail=100

build: ## Build all service images without starting them
	$(COMPOSE) build

test: ## Run the unit test suite with coverage
	pytest --cov=src --cov-report=term-missing

lint: ## Check formatting and lint rules
	ruff check .
	ruff format --check .

format: ## Apply formatting and safe lint fixes
	ruff format .
	ruff check --fix .

typecheck: ## Run mypy
	mypy

check: lint typecheck test ## Everything CI runs

dbt-run: ## Build the dbt models against the DuckDB warehouse
	$(COMPOSE) run --rm --profile transform dbt dbt build --project-dir $(DBT_DIR)

dbt-test: ## Run dbt tests only
	$(COMPOSE) run --rm --profile transform dbt dbt test --project-dir $(DBT_DIR)

clean: ## Remove local caches and generated data (keeps the sample snapshot)
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find data -mindepth 1 -maxdepth 1 ! -name '.gitkeep' ! -name 'sample' -exec rm -rf {} +
