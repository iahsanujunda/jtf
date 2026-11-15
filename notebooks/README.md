# notebooks

Exploration only. No feature logic lives here: a feature worth keeping moves into a dbt model, so there is no pandas-to-SQL rewrite and no train/serve skew.

| Notebook | Reads | Purpose |
| --- | --- | --- |
| [00_source_probe](00_source_probe.ipynb) | The live sources, cached per day in `data/probe/` | How each source was probed, and how every finding became an extraction rule |
| [01_history_probe](01_history_probe.ipynb) | Historical files and APIs, cached per day in `data/probe/` | What history exists, how it differs from today's files, what loading it costs; the design of step 2 |
| Later notebooks | Dev BigQuery tables and MLflow runs | Feature and model exploration |

Run them with the notebook dependencies: `uv sync --group notebook`, then pick the `.venv` kernel in VS Code or Jupyter.
