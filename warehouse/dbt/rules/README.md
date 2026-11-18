# Rules

Checks that keep the warehouse point-in-time correct, run with pytest in CI. Owned by the warehouse because they guard its SQL. Why these and no others: the phase-1 plan's [point-in-time rules](../../../docs/phase1/README.md#point-in-time-rules). Two rules carry everything, `known_at` right for each source and every input read through `as_of`; what each input can use at `T` follows from them.

**Layers.** Every model and seed sits in the dataset of its layer and is named for it (`dim_sun_slots` in `dim`). A model reads only these layers:

| A model in | Reads | Holds |
| --- | --- | --- |
| DIM | DIM; the seeds are DIM | What nothing is learned about over time: the sun, the weather points, the parts of the area, later the holiday calendar. No `known_at`, so every layer reads it directly |
| DWD | ODS, DIM, DWD | Facts with every version and its `known_at`. The only layer that reads ODS |
| DWS | DIM, DWD, DWS | One version per decision time: versioned DWD only through `as_of` |
| ADS | DIM, DWD, DWS, ADS | Outputs, labels among them (targets take the latest values, plan rule 4) |

No layer reads one above it. If DWD read DWS, a fact would depend on what was known at some decision time, and rebuilding a layer from the ones below would no longer give the same answer. Data tests read any layer: they check, they are not read.

| Rule | Checked by | Built | Why |
| --- | --- | --- | --- |
| Each model reads only what its layer may read (the table above), and each model and seed is in a layer and named for it; every DWD model another layer may read documents `known_at` and `known_until`, which `as_of` reads | [test_layers.py](test_layers.py): dbt's `manifest.json`, parsed afresh, the `depends_on` and documented columns of every model | Yes, with no exception since 2026-11-18, when the spine moved from the ODS manifest to `dwd_source_captures`. An exception would be listed in the test, which fails once the read it excuses is gone | ODS has no `known_at`, so a read of it anywhere but DWD bypasses `as_of`; what DWS needs from ODS gets a DWD model first |
| Every versioned DWD model a DWS model reads is joined through `as_of` | [test_as_of.py](test_as_of.py): sqlglot over the SQL `dbt compile` produces, scope by scope. Each read of such a table carries, on its own alias and in its own scope (its join's `ON` or the scope's `WHERE`), the predicate `as_of` writes, both halves against one `T`, a column named `decision_time`. Strict: the same predicate written another way, or applied outside a CTE that reads the table, fails too; write it with the macro, on the table itself. 14 cases tell apart what passes and what fails | Yes, since 2026-11-18, with the spine's read as its one exemption (listed in the test, which fails once it is no longer used) | Every feature is point-in-time correct (F2) |
| No `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()` in any model | [test_clock.py](test_clock.py): sqlglot over every model's compiled SQL (`CURRENT_DATE`, `CURRENT_DATETIME`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `NOW`, nested or not), and a pattern over the raw Jinja of every model and project macro: `run_started_at` or `modules.datetime` compiles to a literal no SQL check can tell from any other timestamp | Yes, since 2026-11-18 | Nothing reads the clock; `T` is always passed in, so a replay gives the same answer whenever it runs |
| Each file's rows in ODS match the fingerprint recorded at their load | Not a rule here: the check `rows_as_loaded` on each ODS asset, after every load ([fingerprint.py](../../../jtf/src/jtf/parse/fingerprint.py)). It reads only the partitions whose row count or last change moved since their last record | Yes, since 2026-11-18 ([notebook 07](../../../notebooks/07_ods_fingerprints.ipynb)). The files loaded before it were fingerprinted then, as `backfill` | ODS is append-only (F3). dbt never writes ODS and BigQuery has no insert-only role, so no rule over dbt's SQL could catch an `UPDATE` |

**Outside the `as_of` rule:**

- **DIM**: no `known_at`, read directly
- **Labels**, in ADS: targets use the latest values (rule 4), so they never go through `as_of`
- **DWS reading DWS**: every DWS model is already keyed by decision time
- **The decision-time spine's read of capture times**, by name: `dws_decision_slots` reads how far the captures reach now. That decides which decision times exist and feeds no value, so nothing at any `T` can learn from it, and old rows stay identical however late the spine is rebuilt. Built from captures known at each `T` instead, a day with a capture outage would vanish from the axis rather than show up as missing weather. The rule lists the exemption, and fails if it is no longer used
- **A model private to a dbt group**: `dwd_capacity_area_files` holds capacity figures per raw file, learned over time but with no `known_at`. It is private to the group `capacity_figures`, so only `dwd_capacity_area`, which makes the versions, can read it: dbt refuses any other `ref` when it parses the project, tests included (the two that check it are in the group)

**Also here:** [test_parse.py](test_parse.py), that dbt parses the whole project every time (`partial_parse: false`), so what a run builds is what is on disk: it renames a unit test in a copy of the project and edits a sources file in the same parse, the change after which a partial parse kept the test under its old name too.

**The spike behind them** (2026-11-18): sqlglot parses all 18 compiled models in BigQuery's dialect, and its scope module tells CTEs from tables. A first checker passed the four `as_of` joins and failed every form that must fail: the predicate on another alias, half of it, `<` for `<=`, the clock or a literal as `T`, two `T`s, an `OR` around it, a "latest" subquery, a second bare read, a read through a CTE. It also fails two correct forms, the comparison's sides swapped and the versions in `FROM` with the spine joined, which no model uses: one way to write it, agreed. The compiled SQL can't come from CI's `dbt build --empty`, which wraps each `ref()` in a subquery of its own, and `dbt compile` needs BigQuery credentials even with `--no-introspect` and `--no-populate-cache`.

**The fingerprint's spike** ([notebook 07](../../../notebooks/07_ods_fingerprints.ipynb)): the manifest recorded nothing of the rows; the sum of `FARM_FINGERPRINT` as `BIGNUMERIC`, over each row as JSON without its nulls, is stable, counts a file loaded twice (`BIT_XOR` cancels it) and leaves old rows alone when a column is added; partition metadata moved exactly where a change was made, so the check after every load reads nothing while nothing changed.

**Where the rules run:** in CI's hermetic gate, with the unit tests and an empty `dbt build`, in a PR dataset that reads no real data. The two over compiled SQL need a compile, which needs BigQuery: the `warehouse` job compiles every model against its own empty datasets (`JTF_DBT_COMPILED` names the result), and `make rules` compiles against dev; `make test` and CI's `jtf` job skip them and run the rest, their cases included. The data tests are not rules: they check real data, after every build, in the pipeline.
