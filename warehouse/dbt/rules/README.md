# Rules

Checks that keep the warehouse point-in-time correct, run with pytest in CI. Owned by the warehouse because they guard its SQL. Why these and no others: the phase-1 plan's [point-in-time rules](../../../docs/phase1/README.md#point-in-time-rules). Two rules carry everything, `known_at` right for each source and every input read through `as_of`; what each input can use at `T` follows from them.

| Rule | Checked by | Why |
| --- | --- | --- |
| No DWS model depends on an ODS source | dbt's `manifest.json`: the `depends_on` of every model under `models/dws` | ODS has no `known_at`, so any read of it bypasses `as_of`. What DWS needs from ODS gets a DWD model first |
| Every versioned DWD model a DWS model reads is joined through `as_of` | sqlglot over the SQL `dbt compile` produces: each such table carries the `known_at` / `known_until` predicate against the decision time | Every feature is point-in-time correct (F2) |
| No `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()` in any model | sqlglot over compiled SQL | Nothing reads the clock; `T` is always passed in, so a replay gives the same answer whenever it runs |
| Each file's rows in ODS match the fingerprint recorded at their load | A dbt data test against `ods.raw_files`, after every load | ODS is append-only (F3). dbt never writes ODS and BigQuery has no insert-only role, so no rule over dbt's SQL could catch an `UPDATE` |

**Outside the `as_of` rule:**

- **Timeless models**, tagged `timeless` in their dbt config: DWD models that hold nothing learned over time (`dwd_sun_slots`, `dwd_daylight`, later the holiday calendar). They have no `known_at` and are read directly
- **Labels**, in their own models outside DWS: targets use the latest values (rule 4), so they never go through `as_of`
- **DWS reading DWS**: every DWS model is already keyed by decision time

**Before building:** two short spikes. Finding the `as_of` predicate per table alias in compiled SQL with sqlglot, including the forms that must fail (the predicate on another table, a "latest" subquery with no `T`). And the fingerprint: what `raw_files` records per file at load, and how cheaply a data test can recompute it.
