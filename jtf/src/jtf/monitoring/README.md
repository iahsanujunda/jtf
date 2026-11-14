# monitoring

| Piece | Phase |
| --- | --- |
| Python checks that dbt tests can't express: freshness against the capture schedule, units and ranges, completeness per day, output sanity | 1 |
| Writing every check result to `monitoring.check_results` | 2 |
| Model health card | 3 |
| AI triage agent: drafts an incident note, recommends, never acts | 5 |

Drift and accuracy are dbt models in [warehouse/dbt/models/monitoring](../../../../warehouse/dbt/models/monitoring/), not Python, so the time machine can replay them.
