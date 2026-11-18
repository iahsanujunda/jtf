"""MLflow, local in phase 1 (plan, Tracking): `mlflow.db` and `mlruns/` at the repository root,
both gitignored, unless MLFLOW_TRACKING_URI names another store. MLflow is the dependency group
`ml`, imported only when a backtest logs, so the capture image never carries it."""

import os
import subprocess
import time
from datetime import date
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
EXPERIMENT = "solar-backtest"


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI") or f"sqlite:///{REPO / 'mlflow.db'}"


def git_state() -> dict[str, str]:
    """The commit the code came from, and whether it had uncommitted changes. Empty in the image,
    which carries no git: its digest names the code there."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
        ).stdout.strip()
        changes = subprocess.run(
            ["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return {}
    return {"git_commit": commit, "git_dirty": str(bool(changes)).lower()}


def run_code() -> dict[str, str]:
    """The code a run comes from: its git commit, whether the tree was clean, its image digest."""
    return {**git_state(), "image_digest": os.environ.get("JTF_IMAGE_DIGEST", "")}


def log_backtest(
    *,
    run_id: str,
    model: str,
    first_month: date,
    last_month: date,
    per_fold: pd.DataFrame,
    overall: dict[str, float],
    fidelity: dict[str, int],
) -> str:
    """One MLflow run per model per backtest: the overall metrics, each metric per fold as a step
    (MLflow draws them over the folds), and the table of folds. Returns MLflow's run ID."""
    import mlflow
    from mlflow.entities import Metric

    mlflow.set_tracking_uri(tracking_uri())
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT)
    experiment_id = (
        experiment.experiment_id
        if experiment
        else client.create_experiment(EXPERIMENT, artifact_location=(REPO / "mlruns").as_uri())
    )
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"{model} {run_id[:8]}") as run:
        mlflow.set_tags(
            {
                "dagster_run_id": run_id,
                "model": model,
                "mode": "backtest",
                "image_digest": os.environ.get("JTF_IMAGE_DIGEST", ""),
                **git_state(),
            }
        )
        mlflow.log_params(
            {
                "first_test_month": first_month.isoformat(),
                "last_test_month": last_month.isoformat(),
                "folds": len(per_fold),
                "slots_scored": "daylight, labelled",
                "exact_decision_times": fidelity["exact"],
                "approximate_decision_times": fidelity["approximate"],
            }
        )
        mlflow.log_metrics(overall)
        now = int(time.time() * 1000)
        client.log_batch(
            run.info.run_id,
            metrics=[
                Metric(key=f"fold_{name}", value=float(value), timestamp=now, step=step)
                for name in ("mae_share", "bias_share", "mae_mw")
                for step, value in enumerate(per_fold[name])
            ],
        )
        mlflow.log_text(per_fold.to_csv(index=False), "folds.csv")
    return run.info.run_id
