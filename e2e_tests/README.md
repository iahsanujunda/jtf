# e2e tests

Only the guarantees no single component can prove alone. Everything else is tested by its owner, so this suite stays small.

| Test | Proves | Components |
| --- | --- | --- |
| Determinism | Replaying the same `T` twice gives identical output | jtf + dbt |
| Future poison | Rows with `known_at` after `T` and absurd values change nothing in a replay at `T`, so nothing bypasses `as_of` | jtf + dbt |
| Reproduce | A what-if run, then a reproduce of that run, match exactly | jtf + dbt + run records |
| One day end to end | A small fixture world goes raw → ODS → DWD → DWS → forecast with the expected shape and checks | all of the pipeline |

Run against a throwaway PR dataset in the dev project, through Workload Identity Federation in CI. They run when `jtf/` or `warehouse/` changes.
