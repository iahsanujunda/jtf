# DWS

Features per (decision time, target slot), materialised for every decision time in one pass, so a full-history backtest is one query. Reads DWD only through the as-of macro. Features are append-only: changing what a feature means adds a new column; old columns go only when no production model uses them.

Phase 1: solar features (weather at the Kanto points, clear-sky output, D−2 lags, calendar, capacity).

What every slot of a decision time shares is a model of its own, one row per decision time: `dws_capacity`, the TEPCO area's installed solar on D as estimated at `T`, the least-squares line through the newest four releases known at `T` by quarter, with the newest figure carried beside it. It reproduces [notebook 03](../../../../notebooks/03_solar_capacity.ipynb)'s scores over the training window exactly (section 7: 0.44% mean error against 1.93% for the carried figure) and is computed in `NUMERIC`, so a rebuild gives the same figures to the bit. The features, the metrics' normalisation by capacity and the range checks read it. `dws_point_weights` holds each weather point's share of the newest release known at `T`: it weights the clear-sky radiation in `dws_solar_features.clear_sky_mw` (capacity × weighted clear sky ÷ 1000 W/m²), and later the weather.
