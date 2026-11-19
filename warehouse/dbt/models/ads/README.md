# ADS

Outputs, by the application they serve: `ads_<application>_<family>_<project>`, grouped in Dagster by application (`ads_ml`). So far one application, the forecast (`ml`); phase 2's dashboard adds `ads_dash_*`. Every layer below may be read here, and nothing below reads ADS: a label read by a feature would be a leak, and the layer rule fails it.

| Family | A row is | Written by | Phase |
| --- | --- | --- | --- |
| `ads_ml_label_solar` | The truth of a slot, from the latest actuals (never `as_of`: targets take the latest values). `label_mw` is delivered + curtailed, what the weather allowed; `delivered_mw` and `curtailed_mw` beside it | dbt | 1 |
| `ads_ml_prediction_solar` | What a model said, per run, model, decision time and slot, appended under the run's ID with its mode (`backtest`, `what-if` or `reproduce`). A dbt source in layer `ads` | Python (`jtf.ml`) | 1 |
| `ads_ml_runs` | One run: its code, its fidelity counts and its fingerprints (below). Read by `jtf.ml`, no dbt model yet | Python (`jtf.ml`) | 1 |
| `ads_ml_day_solar` | A day and the groups results are read by: its clearness against a clear day at that time of year and the sky that gives (clear, mixed, cloudy), whether it was curtailed, its season. Known only afterwards, and from the 14 days after it too: a day's sky is set once those are whole and never changes after. For reading results, never an input, which the layer rule holds it to | dbt | 1 |
| `ads_ml_outcome_solar` | A prediction met by its label: the error in MW and as a share of the capacity estimated at `T`, the daylight flag, and its target day's groups from `ads_ml_day_solar`. The error is defined here once; the backtest's metrics aggregate it, and so will monitoring | dbt | 1 |
| `ads_ml_feature_solar` | The exact inputs a published run read, so a replay can be compared feature by feature. A snapshot, never what models read: they read DWS | Python | 2 |
| `ads_ml_metrics_solar` | A model's accuracy per day, from the outcomes, for monitoring and the dashboard. A backtest's metrics stay in MLflow with its run | dbt | 2 |

Phase 1 publishes no forecast, so backtests write to this dataset, each row under its run's ID. How replays and shadow runs stay apart from the published forecast, a dataset of their own as the PRD says, is decided with the daily forecast in phase 2. Each run has a record in `ads_ml_runs`, written by `jtf.ml` beside its predictions: its mode, the run it reproduces, its model and version, its code (git commit, a clean tree or not, image digest), its decision times and how many of them are exact, a fault if it injected one, fingerprints of the features it read and the predictions it wrote, and when it ran. A run that fails writes none. The fallback level is per forecast, so it will sit on the predictions, with F1 in step 8. MLflow keeps a backtest's metrics, tagged with its fidelity counts.
