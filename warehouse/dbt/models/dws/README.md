# DWS

Features per (decision time, target slot), materialised for every decision time in one pass, so a full-history backtest is one query. Reads DWD only through the as-of macro. Features are append-only: changing what a feature means adds a new column; old columns go only when no production model uses them.

Phase 1: solar features (weather at the Kanto points, clear-sky output, D−2 lags, calendar, capacity).
