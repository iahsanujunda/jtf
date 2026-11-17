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
| Each model reads only what its layer may read (the table above), and each model and seed is in a layer and named for it | [test_layers.py](test_layers.py): dbt's `manifest.json`, parsed afresh, the `depends_on` of every model | Yes. One read breaks it today, listed in the test: `dws_decision_slots` reads the newest daily capture from the ODS manifest until `dwd_source_captures` holds it. The test fails once that read is gone, so the exception can't outlive it | ODS has no `known_at`, so a read of it anywhere but DWD bypasses `as_of`; what DWS needs from ODS gets a DWD model first |
| Every versioned DWD model a DWS model reads is joined through `as_of` | sqlglot over the SQL `dbt compile` produces: each such table carries the `known_at` / `known_until` predicate against the decision time | After its spike | Every feature is point-in-time correct (F2) |
| No `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()` in any model | sqlglot over compiled SQL | After the spike | Nothing reads the clock; `T` is always passed in, so a replay gives the same answer whenever it runs |
| Each file's rows in ODS match the fingerprint recorded at their load | A dbt data test against `ods.raw_files`, after every load | After its spike | ODS is append-only (F3). dbt never writes ODS and BigQuery has no insert-only role, so no rule over dbt's SQL could catch an `UPDATE` |

**Outside the `as_of` rule:**

- **DIM**: no `known_at`, read directly
- **Labels**, in ADS: targets use the latest values (rule 4), so they never go through `as_of`
- **DWS reading DWS**: every DWS model is already keyed by decision time
- **A model private to a dbt group**: `dwd_capacity_area_files` holds capacity figures per raw file, learned over time but with no `known_at`. It is private to the group `capacity_figures`, so only `dwd_capacity_area`, which makes the versions, can read it: dbt refuses any other `ref` when it parses the project, tests included (the two that check it are in the group)

**Before building the rest:** two short spikes. Finding the `as_of` predicate per table alias in compiled SQL with sqlglot, including the forms that must fail (the predicate on another table, a "latest" subquery with no `T`). And the fingerprint: what `raw_files` records per file at load, and how cheaply a data test can recompute it.
