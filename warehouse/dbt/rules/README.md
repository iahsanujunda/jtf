# SQL rules

sqlglot checks over the SQL that `dbt compile` produces, run with pytest. Owned by the warehouse because they guard its SQL.

| Rule | Why |
| --- | --- |
| No `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()` anywhere | Nothing reads the clock; `T` is always passed in |
| DWS models read ODS and DWD only through `as_of` | Every feature is point-in-time correct (F2) |
| No `UPDATE` or `DELETE` against ODS | ODS is append-only (F3) |
