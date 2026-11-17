# ADS

Outputs, in four families. Every layer below may be read here, and nothing below reads ADS: a label read by a feature would be a leak, and the layer rule fails it.

| Family | A row is | Written by | Phase |
| --- | --- | --- | --- |
| `ads_label_solar` | The truth of a slot, from the latest actuals (never `as_of`: targets take the latest values). `label_mw` is delivered + curtailed, what the weather allowed; `delivered_mw` and `curtailed_mw` beside it | dbt | 1 |
| `ads_prediction_solar` | What a model said, per run, model, decision time and slot, appended under the run's ID with its mode (`backtest` now). A dbt source in layer `ads` | Python (`jtf.ml`) | 1 |
| `ads_outcome_solar` | A prediction met by its label: the error in MW and as a share of the capacity estimated at `T`, and the daylight flag. The error is defined here once; the backtest's metrics aggregate it, and so will monitoring | dbt | 1 |
| `ads_feature_*` | The exact inputs a published run read, so a replay can be compared feature by feature. A snapshot, never what models read: they read DWS | Python | 2 |

Phase 1 publishes no forecast, so backtests write to this dataset, each row under its run's ID. How replays and shadow runs stay apart from the published forecast, a dataset of their own as the PRD says, is decided with the daily forecast in phase 2. The record of each run (`ads_runs`: `T`, image digest, model version, fallback level, fidelity) comes with the time machine in step 6; until then MLflow holds a backtest's metadata.
