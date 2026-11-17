"""The backtest as assets, materialized together by hand, since a backtest is a deliberate run:

1. `ads/ads_prediction_solar`: the run's predictions, appended to ADS under its run ID. Keyed as
   dbt's source for that table, so it is one node in the lineage
2. `ads/ads_outcome_solar`: a dbt model, each prediction met by its label
3. `ml/solar_backtest_metrics`: the run's outcome aggregated per fold, logged to MLflow

So the lineage runs from raw files through DWS to the metrics. Folds, the baseline and the metrics
are functions in jtf.ml; these assets only wire them to BigQuery and MLflow.
"""

from datetime import date

import dagster as dg

from jtf.ml.backtest import (
    FIRST_TEST_MONTH,
    PERSISTENCE,
    fold_months,
    last_complete_month,
    metrics,
    month,
    persistence,
)
from jtf.ml.tracking import log_backtest
from jtf.orchestration.resources import Datasets

PREDICTIONS = dg.AssetKey(["ads", "ads_prediction_solar"])
OUTCOMES = dg.AssetKey(["ads", "ads_outcome_solar"])


class BacktestConfig(dg.Config):
    first_test_month: str = FIRST_TEST_MONTH.strftime("%Y-%m")
    last_test_month: str | None = None
    """Default: the newest month with every slot labelled."""


@dg.asset(
    key=PREDICTIONS,
    deps=[dg.AssetKey(["dws", "dws_solar_features"]), dg.AssetKey(["ads", "ads_label_solar"])],
    group_name="ml",
    kinds={"python", "bigquery"},
    description="The persistence baseline's predictions for every decision time of the test months",
)
def solar_predictions(
    context: dg.AssetExecutionContext, config: BacktestConfig, datasets: Datasets
) -> dg.MaterializeResult:
    warehouse = datasets.backtest()
    first = month(config.first_test_month)
    last = (
        month(config.last_test_month)
        if config.last_test_month
        else last_complete_month(warehouse.newest_complete_label_day())
    )
    folds = fold_months(first, last)
    rows = persistence(warehouse.features(first, last), run_id=context.run.run_id)
    warehouse.append_predictions(rows)
    return dg.MaterializeResult(
        metadata={
            "run_id": context.run.run_id,
            "model": PERSISTENCE,
            "first_test_month": first.isoformat(),
            "last_test_month": last.isoformat(),
            "folds": len(folds),
            "rows": len(rows),
        }
    )


@dg.asset(
    key=dg.AssetKey(["ml", "solar_backtest_metrics"]),
    deps=[OUTCOMES],
    group_name="ml",
    kinds={"python", "mlflow"},
    description="The newest backtest run's metrics per fold and overall, logged to MLflow",
)
def solar_backtest_metrics(
    context: dg.AssetExecutionContext, datasets: Datasets
) -> dg.MaterializeResult:
    event = context.instance.get_latest_materialization_event(PREDICTIONS)
    if event is None or event.asset_materialization is None:
        raise dg.Failure("no predictions yet: materialize ads/ads_prediction_solar first")
    run = {k: v.value for k, v in event.asset_materialization.metadata.items()}
    per_fold, overall = metrics(datasets.backtest().outcomes(run["run_id"]))
    if len(per_fold) != run["folds"]:
        raise dg.Failure(f"{len(per_fold)} folds scored, {run['folds']} predicted")
    mlflow_run_id = log_backtest(
        run_id=run["run_id"],
        model=run["model"],
        first_month=date.fromisoformat(run["first_test_month"]),
        last_month=date.fromisoformat(run["last_test_month"]),
        per_fold=per_fold,
        overall=overall,
    )
    context.log.info(f"{run['model']}: {overall}")
    return dg.MaterializeResult(
        metadata={"run_id": run["run_id"], "mlflow_run_id": mlflow_run_id, **overall}
    )


ML_ASSETS = [solar_predictions, solar_backtest_metrics]
