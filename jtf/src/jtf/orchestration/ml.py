"""A model's runs as assets, materialized by hand, since a run is deliberate (jtf.ml.replay):

1. `ads/ads_ml_prediction_solar`: one run's predictions, appended to ADS under its run ID, and its
   record in `ads_ml_runs`. Its config says which run: a backtest over test months, a what-if over
   target days, or a reproduce of an earlier run. Keyed as dbt's source for the predictions, so
   it is one node in the lineage
2. `ads/ads_ml_outcome_solar`: a dbt model, each prediction met by its label
3. `ml/solar_backtest_metrics`: a backtest's outcome aggregated per fold and per kind of day,
   logged to MLflow

So the lineage runs from raw files through DWS to the metrics. Folds, the models, runs and the
metrics are functions in jtf.ml; these assets only wire them to BigQuery and MLflow.
"""

from datetime import UTC, date, datetime

import dagster as dg
import pandas as pd

from jtf.ml.backtest import (
    FIRST_TEST_MONTH,
    PERSISTENCE,
    by_group,
    fold_months,
    last_complete_month,
    metrics,
    month,
)
from jtf.ml.replay import Mismatch, Refused, reproduce, run, target_day
from jtf.ml.tracking import FINGERPRINTS, links, log_backtest, log_trainings, run_code
from jtf.orchestration.resources import Datasets

PREDICTIONS = dg.AssetKey(["ads", "ads_ml_prediction_solar"])
OUTCOMES = dg.AssetKey(["ads", "ads_ml_outcome_solar"])


class RunConfig(dg.Config):
    mode: str = "backtest"
    """backtest, what-if or reproduce."""
    model: str = PERSISTENCE
    first_test_month: str = FIRST_TEST_MONTH.strftime("%Y-%m")
    last_test_month: str | None = None
    """A backtest's months; the last by default the newest month with every slot labelled."""
    first_target_day: str | None = None
    last_target_day: str | None = None
    """A what-if's target days, YYYY-MM-DD."""
    reproduces: str | None = None
    """A reproduce's original run ID."""
    fault: str | None = None
    """A fault to inject; none exist until step 8."""


def _days(config: RunConfig, warehouse) -> tuple[date, date]:
    if config.mode == "backtest":
        last = (
            month(config.last_test_month)
            if config.last_test_month
            else last_complete_month(warehouse.newest_complete_label_day())
        )
        fold_months(month(config.first_test_month), last)  # refuses an empty range
        end = (pd.Timestamp(last) + pd.offsets.MonthEnd()).date()
        return month(config.first_test_month), end
    if not (config.first_target_day and config.last_target_day):
        raise Refused("a what-if needs first_target_day and last_target_day")
    return date.fromisoformat(config.first_target_day), date.fromisoformat(config.last_target_day)


@dg.asset(
    key=PREDICTIONS,
    deps=[dg.AssetKey(["dws", "dws_solar_features"]), dg.AssetKey(["ads", "ads_ml_label_solar"])],
    group_name="ads_ml",  # an ADS table: grouped by its application, as dbt's are (dbt.group_of)
    kinds={"python", "bigquery"},
    description="One run of a model: its predictions for every decision time asked, and its record",
)
def solar_predictions(
    context: dg.AssetExecutionContext, config: RunConfig, datasets: Datasets
) -> dg.MaterializeResult:
    warehouse, run_id, code = datasets.backtest(), context.run.run_id, run_code()
    now = lambda: datetime.now(UTC)  # noqa: E731
    try:
        if config.mode == "reproduce":
            if not config.reproduces:
                raise Refused("a reproduce needs the run it reproduces")
            record = reproduce(
                warehouse, run_id=run_id, original=config.reproduces, code=code, now=now
            )
        else:
            first, last = _days(config, warehouse)
            record = run(
                warehouse,
                run_id=run_id,
                mode=config.mode,
                model=config.model,
                first_day=first,
                last_day=last,
                code=code,
                now=now,
                fault=config.fault,
            )
    except (Refused, Mismatch) as e:
        raise dg.Failure(description=str(e)) from e
    # What the run was made from, kept with the materialization: the metrics read it from there
    made_from = {
        name: record[name]
        for name in ["git_commit", "tree_dirty", *FINGERPRINTS]
        if pd.notna(record[name])
    } | ({"git_url": code["git_url"]} if code.get("git_url") else {})
    opened = links(record | made_from)
    trained = {}
    if record["trainings"]:  # a trained model: each month's training logged, its files kept
        logged, link = log_trainings(
            run=record | made_from, trainings=record["trainings"], rows=record["training_rows"]
        )
        trained = {
            "trainings": len(record["trainings"]),
            "trainings_mlflow_run_id": logged,
            "trainings_in_mlflow": dg.MetadataValue.url(link),
        }
    return dg.MaterializeResult(
        metadata={
            **trained,
            **made_from,
            **({"code": dg.MetadataValue.url(opened["code"])} if "code" in opened else {}),
            "run_id": run_id,
            "mode": record["mode"],
            "model": record["model"],
            "first_target_day": target_day(record["first_decision_time"]).isoformat(),
            "last_target_day": target_day(record["last_decision_time"]).isoformat(),
            "decision_times": record["decision_times"],
            "exact": record["exact"],
            "approximate": record["approximate"],
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
        raise dg.Failure("no predictions yet: materialize ads/ads_ml_prediction_solar first")
    run = {k: v.value for k, v in event.asset_materialization.metadata.items()}
    if run.get("mode") != "backtest":
        raise dg.Failure(f"the newest run is a {run.get('mode')}, not a backtest: nothing to score")
    first_month = date.fromisoformat(run["first_target_day"]).replace(day=1)
    last_month = date.fromisoformat(run["last_target_day"]).replace(day=1)
    folds = fold_months(first_month, last_month)
    outcomes = datasets.backtest().outcomes(run["run_id"])
    per_fold, overall = metrics(outcomes)
    groups = by_group(outcomes)
    if len(per_fold) != len(folds):
        raise dg.Failure(f"{len(per_fold)} folds scored, {len(folds)} predicted")
    mlflow_run_id, link = log_backtest(
        run=run,
        first_month=first_month,
        last_month=last_month,
        per_fold=per_fold,
        overall=overall,
        groups=groups,
        fidelity={"exact": run["exact"], "approximate": run["approximate"]},
    )
    context.log.info(f"{run['model']}: {overall}")
    sky = groups[groups["grouping"] == "sky"]
    by_sky = {
        f"mae_share_{g}": float(v) for g, v in zip(sky["group"], sky["mae_share"], strict=True)
    }
    return dg.MaterializeResult(
        metadata={
            "run_id": run["run_id"],
            "mlflow_run_id": mlflow_run_id,
            "in_mlflow": dg.MetadataValue.url(link),
            **overall,
            **by_sky,
        }
    )


ML_ASSETS = [solar_predictions, solar_backtest_metrics]

# Launched by name with their config: a replay (a what-if or a reproduce) and a backtest, scored
METRICS = dg.AssetKey(["ml", "solar_backtest_metrics"])
REPLAY = dg.define_asset_job(
    "replay",
    selection=dg.AssetSelection.assets(PREDICTIONS, OUTCOMES),
    description="A what-if run over target days, or a reproduce of an earlier run",
    config={"ops": {solar_predictions.op.name: {"config": {"mode": "what-if"}}}},
)
BACKTEST = dg.define_asset_job(
    "backtest",
    selection=dg.AssetSelection.assets(PREDICTIONS, OUTCOMES, METRICS),
    description="The rolling backtest over test months, its metrics logged to MLflow",
)
ML_JOBS = [REPLAY, BACKTEST]
