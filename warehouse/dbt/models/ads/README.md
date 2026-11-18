# ADS

Outputs, in four families. Every layer below may be read here, and nothing below reads ADS: a label read by a feature would be a leak, and the layer rule fails it.

| Family | A row is | Written by | Phase |
| --- | --- | --- | --- |
| `ads_label_solar` | The truth of a slot, from the latest actuals (never `as_of`: targets take the latest values). `label_mw` is delivered + curtailed, what the weather allowed; `delivered_mw` and `curtailed_mw` beside it | dbt | 1 |
| `ads_prediction_solar` | What a model said, per run, model, decision time and slot, appended under the run's ID with its mode (`backtest`, `what-if` or `reproduce`). A dbt source in layer `ads` | Python (`jtf.ml`) | 1 |
| `ads_runs` | One run: its code, its fidelity counts and its fingerprints (below). Read by `jtf.ml`, no dbt model yet | Python (`jtf.ml`) | 1 |
| `ads_outcome_solar` | A prediction met by its label: the error in MW and as a share of the capacity estimated at `T`, and the daylight flag. The error is defined here once; the backtest's metrics aggregate it, and so will monitoring | dbt | 1 |
| `ads_feature_*` | The exact inputs a published run read, so a replay can be compared feature by feature. A snapshot, never what models read: they read DWS | Python | 2 |

Phase 1 publishes no forecast, so backtests write to this dataset, each row under its run's ID. How replays and shadow runs stay apart from the published forecast, a dataset of their own as the PRD says, is decided with the daily forecast in phase 2. Each run has a record in `ads_runs`, written by `jtf.ml` beside its predictions: its mode, the run it reproduces, its model and version, its code (git commit, a clean tree or not, image digest), its decision times and how many of them are exact, a fault if it injected one, fingerprints of the features it read and the predictions it wrote, and when it ran. A run that fails writes none. The fallback level is per forecast, so it will sit on the predictions, with F1 in step 8. MLflow keeps a backtest's metrics, tagged with its fidelity counts.
