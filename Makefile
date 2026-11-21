# Local shortcuts only. CI calls the same tools directly.
.PHONY: setup lint fmt test rules e2e image dagster dagster-update where dbt main-state sql own-place try

IMAGE ?= jtf:local
# Settings are two gitignored files in the main checkout. `.env` is what runs there: Dagster, on
# main, writing the warehouse that is kept. `.env.dev` is for work in a worktree: the dev project
# and a prefix of your own. Until `.env.dev` exists a worktree reads `.env`, as before the split.
MAIN := $(abspath $(shell git rev-parse --path-format=absolute --git-common-dir)/..)
MAIN_ENV := $(MAIN)/.env
DEV_ENV := $(MAIN)/.env.dev
ENV_FILE := $(if $(and $(filter-out $(MAIN),$(CURDIR)),$(wildcard $(DEV_ENV))),$(DEV_ENV),$(MAIN_ENV))
# A settings file is sourced, never handed to `uv run --env-file`: that flag leaves a variable the
# shell already exports as it is, so a shell that once loaded main's settings would send a trial
# build into main's datasets. Sourced, the file wins
WITH_MAIN := set -a; . $(MAIN_ENV); set +a;
WITH_ENV := set -a; . $(ENV_FILE); set +a;
# Where a set of settings writes: its project and its prefix
TARGET := echo "$$JTF_BIGQUERY_PROJECT.$$JTF_DBT_DATASET_PREFIX"

setup:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

# With the group ml: the trained models' tests need LightGBM
test:
	uv run --group ml pytest

# The warehouse rules with this checkout's settings, those over compiled SQL included: compiling
# reads no data but needs BigQuery, so `make test` skips them
rules:
	$(WITH_ENV) JTF_RULES_COMPILE=1 uv run pytest warehouse/dbt/rules

# The e2e tests (e2e_tests/README.md) with this checkout's settings: their world loaded into
# throwaway datasets, dropped after. A few minutes, a few GB billed
e2e:
	$(WITH_ENV) JTF_DBT_DATASET_PREFIX=ci_$$(date +%s)_e2e uv run pytest e2e_tests

image:
	docker build -t $(IMAGE) .

# Dagster on the laptop: UI on http://localhost:3000, state kept in .dagster/, settings from .env;
# with MLflow (the group ml) for the backtest.
# Before it starts: refused against prod unless this is a clean checkout of main; Dagster's own
# settings written from deploy/dagster; the tables Python writes made as the code has them
dagster:
	mkdir -p .dagster
	$(WITH_MAIN) uv run jtf dagster ready
	$(WITH_MAIN) uv run jtf warehouse ensure
	$(WITH_MAIN) DAGSTER_HOME=$(CURDIR)/.dagster uv run --group ml dagster dev -m jtf.orchestration.definitions

# Pull main into the checkout Dagster runs from, with no run in between (see AGENTS.md)
dagster-update:
	$(WITH_MAIN) uv run jtf dagster update

# Where this checkout's settings write: project.prefix
where:
	@$(WITH_ENV) $(TARGET)

# dbt with this checkout's settings, e.g. make dbt ARGS="build --select dwd".
# Refused against prod unless this is a clean checkout of main
dbt:
	$(WITH_ENV) uv run jtf warehouse guard
	$(WITH_ENV) cd warehouse/dbt && uv run dbt $(or $(ARGS),build) --profiles-dir .

# Where every model lives in the warehouse main builds: dbt's parsed project under main's settings.
# A build in dev reads from there whatever it doesn't build itself
main-state:
	$(WITH_MAIN) cd warehouse/dbt && uv run dbt parse --profiles-dir . --target-path target/main-state

# A changed model as it will run, every table name and variable resolved: make sql MODEL=dws_capacity
sql: main-state
	$(WITH_ENV) cd warehouse/dbt && uv run dbt compile --select $(MODEL) --defer --state target/main-state --profiles-dir .

# A trial must have a place of its own: refused when these settings write where main does
own-place:
	@main=$$($(WITH_MAIN) $(TARGET)); mine=$$($(WITH_ENV) $(TARGET)); \
	if [ "$$main" = "$$mine" ]; then \
		echo "these settings write where main does ($$main): give $(DEV_ENV) a project or a prefix of its own (docs/workflow.md)"; \
		exit 1; \
	fi

# Changed models built and tested in datasets of your own on real data, reading everything else
# from the warehouse main builds: make try MODELS="dws_capacity dws_solar_features"
try: own-place main-state
	$(WITH_ENV) cd warehouse/dbt && uv run dbt build --select $(MODELS) --defer --state target/main-state --profiles-dir .
