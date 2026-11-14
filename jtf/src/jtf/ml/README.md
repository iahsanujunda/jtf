# ml

Models and everything that runs them.

| Piece | Phase |
| --- | --- |
| Baselines: persistence (D−2 same slot), clear-sky × recent ratio | 1 |
| TEPCO forecast as a benchmark (published after the gate, wind + solar combined) | 1 |
| M1 solar: LightGBM quantile models for P10/P50/P90 | 1 |
| Rolling-origin backtest, features read as of each decision time | 1 |
| Replay runner (the time machine): reproduce and what-if modes | 1 |
| Metrics: MAE (normalised by capacity), pinball loss, P10–P90 coverage, bias, skill vs persistence | 1 |
| M2 price model | 4 |

Determinism is required, not hoped for: `T` passed in, fixed seeds, LightGBM deterministic mode with a fixed thread count, inputs sorted before training and prediction. Every run logs its git commit, image digest and `T` to MLflow (local SQLite until phase 2).
