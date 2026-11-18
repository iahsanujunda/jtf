# Local shortcuts only. CI calls the same tools directly.
.PHONY: setup lint fmt test rules e2e image dagster dagster-update dbt

IMAGE ?= jtf:local
# Settings live in the main checkout's .env, gitignored, so worktrees read that one
ENV_FILE := $(abspath $(shell git rev-parse --path-format=absolute --git-common-dir)/../.env)

setup:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest

# The warehouse rules with the settings in .env, those over compiled SQL included: compiling reads
# no data but needs BigQuery, so `make test` skips them
rules:
	set -a; . $(ENV_FILE); set +a; JTF_RULES_COMPILE=1 uv run pytest warehouse/dbt/rules

# The e2e tests (e2e_tests/README.md) with the settings in .env: their world loaded into throwaway
# datasets in the dev project, dropped after. A few minutes, a few GB billed
e2e:
	set -a; . $(ENV_FILE); set +a; JTF_DBT_DATASET_PREFIX=ci_$$(date +%s)_e2e uv run pytest e2e_tests

image:
	docker build -t $(IMAGE) .

# Dagster on the laptop: UI on http://localhost:3000, state kept in .dagster/, settings from .env;
# with MLflow (the group ml) for the backtest
dagster:
	mkdir -p .dagster
	DAGSTER_HOME=$(CURDIR)/.dagster uv run --group ml dagster dev -m jtf.orchestration.definitions

# Pull main into the checkout Dagster runs from, with no run in between (see AGENTS.md)
dagster-update:
	uv run jtf dagster update

# dbt against dev BigQuery with the settings in .env, e.g. make dbt ARGS="build --select dwd"
dbt:
	cd warehouse/dbt && uv run --env-file $(ENV_FILE) dbt $(or $(ARGS),build) --profiles-dir .
