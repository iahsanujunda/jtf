# How a change travels

The standard for everyone who changes this repository, people and agents alike: where to work, how a change is proven, how it reaches the running pipeline, and what to check after. The detail lives in each component's README, linked from here. Each stage is marked:
- **settled**: done many times;
- **new**: it works but may still change;
- **not yet**: not practised; the [plan](phase1/README.md) or the [PRD](prd.md) holds the target, not a standard.

## Where you work

- **In a git worktree, on a branch.** The main checkout is Dagster's deployment: Dagster reads its asset graph at a reload but dbt's SQL at every run, so any change to that tree without a reload runs new SQL on the old graph. Bring it up to `main` only with `make dagster-update` (below)
- **Against the dev project:** dev BigQuery (`ods` and dbt's `dev_*` datasets), and the prod raw bucket, read-only. Settings live in the main checkout's `.env`, gitignored, with their names in [settings.example.env](../deploy/contract/settings.example.env); nothing account-specific goes in code. `make dbt` and `make rules` read that `.env` from any worktree
- **One Dagster at a time, and no `jtf parse` while it runs:** a parse appends without a lock, so two would load the same file twice

## A new source (settled)

1. **Explore it in a numbered notebook** ([notebooks](../notebooks/README.md)): probe the live source and its history. Commentary in Step / Observed / Source / Learned form, written after reading the outputs, every claim about the source linked and quoted (translated from Japanese). It ends with *What this exploration made known* (We assumed / We found / So) and *Still open*. A notebook never asks for decisions: a disagreement with a "So" is talked through, the notebook rewritten, and the PRD and plan updated to match
2. **Capture it:** a `Source` in [ingest](../jtf/src/jtf/ingest/README.md), with offline tests; a job and its schedule in `capture_sources` ([forecast_env](../deploy/terraform/modules/forecast_env/variables.tf)). Its health check `jtf-<source>` is created by the first ping: then set its cron and grace on healthchecks.io. History comes in as one-off executions of the job with `--initial`; a source that rewrites past files also gets a monthly re-capture
3. **Release the capture by hand**, until `release.yml` exists (new): build and push the image, set its digest in prod's `terraform.tfvars`, `terraform plan` and `apply` ([progress, How to](phase1/progress.md#how-to)). Then run the job once and read its log: capture runs in prod, so a bad image misses a day that can't be fetched again for some sources

## A parser (settled)

1. One parser per file layout ([parse](../jtf/src/jtf/parse/README.md)): it changes the format, never the meaning. Offline tests on fixtures cut from real raw files
2. **Prove it on every raw file** of its source with `jtf parse` into dev ODS, with Dagster stopped
3. **Then orchestrate it** by listing its table in `ORCHESTRATED` ([ods.py](../jtf/src/jtf/orchestration/ods.py)). Changing what a parser writes bumps its `VERSION`: the new rows are appended under it, and ODS is never updated

## A warehouse model (settled)

Read the plan's paragraph for the layer first: units and names come from the plan, not from the source. Write exploratory SQL the way dbt will run it, so a finding moves into a model without a rewrite.

- **DWD** ([dwd](../warehouse/dbt/models/dwd/README.md)): power in MW, slots by their start in JST, every version with `known_at`, `known_until` and `basis`. `known_at` is proven by unit tests: a late backfill, capture start, the same value seen again, a correction, a re-parse. It is also checked on real data after every load: `rule` rows before the source's capture start, `captured` rows at a fetch holding the value, no `known_at` after the newest fetch. Only the actuals have all three so far
- **DWS** ([dws](../warehouse/dbt/models/dws/README.md)): versioned DWD only through `as_of`, DIM directly. A feature earns its place in a notebook first (from 06 on, reading the models through `as_of`); then its logic lives only in dbt, never in pandas. Features are append-only: changing what one means adds a column
- **Any model:** a sum that is compared or versioned is exact (`NUMERIC`, or terms in a fixed order). An incremental model equals a full build, row for row, and its cost is read from BigQuery's job history. Every BigQuery call names the project
- **The rules** ([rules](../warehouse/dbt/rules/README.md)): layers read only downward, DWS reads versioned DWD through `as_of`, and nothing reads the clock. `make test` runs what needs no BigQuery, `make rules` runs all of them, and CI runs them on every pull request that touches the warehouse

## Models and ADS (not yet)

Labels and outcomes are dbt models and predictions are appended by `jtf.ml` under a run ID ([ads](../warehouse/dbt/models/ads/README.md), [ml](../jtf/src/jtf/ml/README.md)); so far there is one model, the persistence baseline. Training, model versions and their release are step 7's to settle.

## Before every commit (settled)

- **`make lint` and `make test`, whatever the change touches**, notebooks and docs included: CI runs only on the paths its filter lists. Add `make rules` when the warehouse changes
- **A new test must be seen to fail:** remove the logic it guards, run it, watch it fail, then restore the logic. A test that can't fail proves nothing

## Release (new)

1. **A pull request:** `main` takes changes only through one. CI runs the `jtf` job (ruff, unit tests, Dagster's definitions) and, for the warehouse, the `warehouse` job (unit tests, an empty build and the rules, in datasets of its own that read no real data) ([workflows](../.github/workflows/README.md))
2. **After the merge, in the main checkout: `make dagster-update`.** It stops the sensors, waits for running runs, pulls, reloads the code location and starts the sensors again. If dependencies changed it refuses: stop `make dagster`, then `git pull`, `make setup` and `make dagster`
3. **Check after, in the data:**
   - A changed model rebuilds only when one of its inputs updates, so materialize it and what reads it in the UI (not group `ml`), then look at its rows, not only its checks: a table that is consistently behind passes every check of it agreeing with itself
   - A new asset or check shows in the UI
   - A change to the daily chain is confirmed by the next morning's run: captures at 08:45, one ODS run, one run of the models, every check green
4. **Prod Dagster, backfills after a release, rollback** (not yet): step 4. The PRD's [development and release workflow](prd.md#development-and-release-workflow) holds the target

## When something fails (settled)

- **A capture failed or is late:** its health check says so. Run the job again (`gcloud run jobs execute`); a capture seen again adds no version, so a rerun is safe
- **A parser refused a file:** the manifest records it as `failed` with the reason, and its partition fails by name. The layout changed, so it needs a new parser version, proven on every raw file
- **A check failed:** find why before rebuilding. Everything above ODS can be rebuilt, so the data is never lost, only wrong for a while. Record what happened and what fixed it in the [progress log](phase1/progress.md#log)

## Keeping this page

It changes when a step's command or order changes. Incidents go to the progress log and its lessons, and reach this page only when they change what someone does at a step. A stage marked *not yet* becomes a standard once it has been done.
