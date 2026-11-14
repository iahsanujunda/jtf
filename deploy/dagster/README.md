# dagster

Prod Dagster deployment on the Compute Engine VM. Local development needs none of this: `dagster dev` runs with local storage and in-process runs.

| File (planned) | Holds |
| --- | --- |
| `dagster.prod.yaml` | Postgres storage on Cloud SQL, Docker run launcher, connection pool limits for `db-f1-micro`, retention for sensor and schedule ticks |
| `compose.prod.yaml` | The VM's services: webserver, daemon, code location (the app image by digest), Cloud SQL Auth Proxy |

The webserver has no login and is never public: it is reached through an IAP tunnel. If Dagster or its database is down, the daily forecast still has the fallback job, which needs neither.
