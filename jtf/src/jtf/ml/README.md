# ml

Models and the backtest that scores them. Features come from DWS and labels from ADS, both built by dbt; nothing here computes a feature.

| Piece | Where | State |
| --- | --- | --- |
| Folds: calendar months of target days, from March 2025 (12 months after the first target day) to the newest month with every slot labelled | [backtest.py](backtest.py) | Built |
| Persistence: the same slot on D−2, the newest day complete at `T` | [backtest.py](backtest.py) | Built |
| Metrics on daylight slots with a label, per fold and overall: MAE and bias as shares of the capacity at `T`, MAE in MW. The error itself is defined in SQL (`ads_outcome_solar`) | [backtest.py](backtest.py) | Built |
| Reading features, labels and outcomes; appending predictions to `ads_prediction_solar` | [warehouse.py](warehouse.py) | Built |
| MLflow, local: `mlflow.db` and `mlruns/` at the repository root (gitignored), experiment `solar-backtest`, one run per model per backtest | [tracking.py](tracking.py) | Built |
| Skill against persistence, the clear-sky × ratio baseline, the TEPCO benchmark, M1 (LightGBM quantile models), pinball loss and coverage, metrics by segment | | Step 7 |
| Replay runner (the time machine): reproduce and what-if modes | | Step 6 |
| M2 price model | | Phase 4 |

A backtest runs in Dagster, by hand: `ads/ads_prediction_solar`, `ads/ads_outcome_solar` and `ml/solar_backtest_metrics` materialized together (see [orchestration](../orchestration/README.md)). MLflow is the dependency group `ml`, imported only when a backtest logs, so the capture image doesn't carry it; `make dagster` adds it. MLflow's UI: `uv run --group ml mlflow ui --backend-store-uri sqlite:///mlflow.db`.

Determinism is required, not hoped for: `T` passed in, inputs sorted before predicting and before every mean, fixed seeds and LightGBM's deterministic mode with a fixed thread count once there is a trained model. Every run logs its git commit (and whether the tree had uncommitted changes), its image digest when there is one, and its months; its fidelity (`exact` or `approximate` by decision time, plan, Time machine) once `dwd_source_captures` gives capture start, and plots in step 7.
