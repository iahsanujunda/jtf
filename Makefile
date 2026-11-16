# Local shortcuts only. CI calls the same tools directly.
.PHONY: setup lint fmt test image dagster dbt

IMAGE ?= jtf:local

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

image:
	docker build -t $(IMAGE) .

# Dagster on the laptop: UI on http://localhost:3000, state kept in .dagster/, settings from .env
dagster:
	mkdir -p .dagster
	DAGSTER_HOME=$(CURDIR)/.dagster uv run dagster dev -m jtf.orchestration.definitions

# dbt against dev BigQuery with the settings in .env, e.g. make dbt ARGS="build --select dwd"
dbt:
	cd warehouse/dbt && uv run --env-file ../../.env dbt $(or $(ARGS),build) --profiles-dir .
