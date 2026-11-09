SHELL   := /bin/bash
COMPOSE := docker compose
DAG     := daily_orders_pipeline
DATE    ?= $(shell date -u +%Y-%m-%d)
CH      := $(COMPOSE) exec -T clickhouse sh -c 'clickhouse-client -u "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" -d analytics --multiquery'

.DEFAULT_GOAL := help
.PHONY: help setup up down restart ps logs trigger backfill query test lint format clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: ## Create .env and local folders (runs automatically before `up`)
	@test -f .env || { cp .env.example .env && echo "AIRFLOW_UID=$$(id -u)" >> .env && echo "created .env"; }
	@mkdir -p data logs

up: setup ## Build images and start the whole stack
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Airflow UI : http://localhost:8080  (admin / admin)"
	@echo "ClickHouse : http://localhost:8123/play"

down: ## Stop the stack (keeps data)
	$(COMPOSE) down

restart: down up ## Restart the stack

ps: ## Show container status and health
	$(COMPOSE) ps

logs: ## Follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

trigger: ## Run the pipeline for one day: make trigger DATE=2026-11-08
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG) -e $(DATE)

backfill: ## Load a date range: make backfill START=2026-11-01 END=2026-11-07
	@test -n "$(START)" -a -n "$(END)" || { echo "usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD"; exit 1; }
	$(COMPOSE) exec airflow-scheduler airflow dags backfill -s $(START) -e $(END) $(DAG)

query: ## Run the example analytics queries against ClickHouse
	$(CH) < sql/analytics.sql

test: ## Run unit tests (Spark tests need Java 17)
	python -m pytest -q

lint: ## Lint and check formatting
	ruff check .
	ruff format --check .

format: ## Auto-format the code
	ruff check --fix .
	ruff format .

clean: ## Stop the stack and delete ALL local data (volumes, data/, logs/)
	$(COMPOSE) down -v
	rm -rf data logs
