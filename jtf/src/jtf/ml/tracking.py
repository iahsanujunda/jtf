"""MLflow, local in phase 1 (plan, Tracking): `mlflow.db` and `mlruns/` at the repository root,
both gitignored, unless MLFLOW_TRACKING_URI names another store. MLflow is the dependency group
`ml`, imported only when a backtest logs, so the capture image never carries it.

The record of a run is its row in `ads_ml_runs`, and its code is its commit. What is logged here
is a copy to read by, keyed by the run ID: each entry says what code and what data its run came
from, and links to the commit and to the run in Dagster."""

import json
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
EXPERIMENT = "solar-backtest"
TRAININGS = "solar-training"
# What a backtest is scored by; the last two only for a run with a range
SCORES = ["mae_share", "bias_share", "mae_mw", "coverage", "pinball_share", "mae_share_priced"]
BENCHMARK = "solar-benchmark"


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI") or f"sqlite:///{REPO / 'mlflow.db'}"


def artifact_root() -> str:
    """Where a new experiment keeps its runs' files: the bucket JTF_MLFLOW_ARTIFACTS names, as
    in prod, else `mlruns/` beside the code. An experiment keeps the place it was made with."""
    return os.environ.get("JTF_MLFLOW_ARTIFACTS") or (REPO / "mlruns").as_uri()


# Where the two UIs are, for the links between a run in one and its entry in the other
DAGSTER_URL, MLFLOW_URL = "http://localhost:3000", "http://localhost:5000"
FINGERPRINTS = ["inputs_fingerprint", "training_fingerprint", "outputs_fingerprint"]
# What a run's training rows are read from (warehouse.BacktestWarehouse.training)
TRAINING_TABLES = ["dws_solar_features", "ads_ml_label_solar", "dim_daylight"]


def _git(*args: str) -> str:
    done = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True)
    return done.stdout.strip()


def web_url(remote: str) -> str:
    """A git remote as the address of its page: `git@host:owner/repo.git` and
    `https://user@host/owner/repo.git` both give `https://host/owner/repo`."""
    address = re.sub(r"^(?:ssh://)?git@([^:/]+)[:/]", r"https://\1/", remote)
    return re.sub(r"^(https?://)[^@/]+@", r"\1", address).removesuffix(".git")


def git_state() -> dict[str, str]:
    """The commit the code came from, whether it had uncommitted changes, and where the
    repository is published, if it has a remote. Empty in the image, which carries no git: its
    digest names the code there."""
    try:
        state = {
            "git_commit": _git("rev-parse", "HEAD"),
            "git_dirty": str(bool(_git("status", "--porcelain"))).lower(),
        }
    except (OSError, subprocess.CalledProcessError):
        return {}
    try:
        return state | {"git_url": web_url(_git("remote", "get-url", "origin"))}
    except subprocess.CalledProcessError:
        return state


def links(run: Mapping) -> dict[str, str]:
    """Where a run's code and its Dagster run can be opened: the commit's page, if the repository
    is published, and the run in Dagster's UI (JTF_DAGSTER_URL, else the local one)."""
    dagster = os.environ.get("JTF_DAGSTER_URL") or DAGSTER_URL
    found = {"dagster_run": f"{dagster.rstrip('/')}/runs/{run['run_id']}"}
    if run.get("git_url") and run.get("git_commit"):
        found["code"] = f"{run['git_url']}/tree/{run['git_commit']}"
    return found


def mlflow_link(experiment_id: str, mlflow_run_id: str) -> str:
    """An entry in MLflow's UI (JTF_MLFLOW_URL, else the local one)."""
    base = (os.environ.get("JTF_MLFLOW_URL") or MLFLOW_URL).rstrip("/")
    return f"{base}/#/experiments/{experiment_id}/runs/{mlflow_run_id}"


def identity(run: Mapping) -> dict[str, str]:
    """What an MLflow entry says about the run it belongs to, as tags: the run's commit and its
    fingerprints as it recorded them (not the code that happens to be checked out when the entry
    is written), MLflow's own tags for the source, which it can't fill itself for a run Dagster
    started, and a description with the links. `run` is the run's record, with `git_url` if the
    repository is published."""
    commit = run.get("git_commit") or ""
    dirty = str(run.get("tree_dirty")).lower() == "true"
    found = links(run)
    prints = {name: str(run[name]) for name in FINGERPRINTS if pd.notna(run.get(name))}
    source = {"git_commit": commit, "git_dirty": str(dirty).lower()}
    if commit:
        source |= {"mlflow.source.git.commit": commit, "mlflow.source.type": "PROJECT"}
    if run.get("git_url"):
        url = run["git_url"]
        source |= {"mlflow.source.name": url, "mlflow.source.git.repoURL": url}
    code = f"[`{commit[:7]}`]({found['code']})" if "code" in found else f"`{commit[:7] or '?'}`"
    changed = ", **with uncommitted changes**: the commit is not all of it" if dirty else ""
    note = [
        f"Code: {code}{changed}",
        f"Dagster run: [`{str(run['run_id'])[:8]}`]({found['dagster_run']})",
        *[f"{name.replace('_', ' ').capitalize()}: `{value}`" for name, value in prints.items()],
    ]
    return prints | source | {"mlflow.note.content": "\n\n".join(note)}


def run_code() -> dict[str, str]:
    """The code a run comes from: its git commit, whether the tree was clean, its image digest."""
    return {**git_state(), "image_digest": os.environ.get("JTF_IMAGE_DIGEST", "")}


def group_metrics(groups: pd.DataFrame) -> dict[str, float]:
    """A backtest's metrics per kind of day, named `<grouping>/<group>/<metric>`."""
    scores = [name for name in SCORES if name in groups]
    long = groups.melt(id_vars=["grouping", "group"], value_vars=scores).dropna(subset="value")
    names = long["grouping"] + "/" + long["group"] + "/" + long["variable"]
    return dict(zip(names, long["value"].astype(float), strict=True))


def log_trainings(*, run: Mapping, trainings: list[dict], rows: pd.DataFrame) -> tuple[str, str]:
    """One MLflow run for the trainings of one run of a trained model: per month of target days,
    what it learned from (its last day, its size, its fingerprint), how the training went, the
    trained model's file and the file's hash; and once, the rows they all learned from (each
    month's set is those rows up to its last day) with what they were read from. The files are
    for looking into a past month's model: a reproduce trains again and never reads them.
    Returns MLflow's run ID and a link to it."""
    import mlflow
    from mlflow.entities import Metric

    mlflow.set_tracking_uri(tracking_uri())
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(TRAININGS)
    experiment_id = (
        experiment.experiment_id
        if experiment
        else client.create_experiment(TRAININGS, artifact_location=artifact_root())
    )
    run_id, model = run["run_id"], run["model"]
    table = pd.DataFrame(trainings).drop(columns="file")
    numbers = table.drop(columns=["fold", "last_training_day", "sha256", "training_fingerprint"])
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"{model} {run_id[:8]}") as logged:
        mlflow.set_tags(
            {
                "dagster_run_id": run_id,
                "model": model,
                "image_digest": os.environ.get("JTF_IMAGE_DIGEST", ""),
                **identity(run),
            }
        )
        mlflow.log_params(
            {
                "trainings": len(table),
                "first_fold": str(table["fold"].iloc[0]),
                "last_fold": str(table["fold"].iloc[-1]),
            }
        )
        now = int(time.time() * 1000)
        long = numbers.reset_index().melt(id_vars="index").dropna(subset="value")
        client.log_batch(
            logged.info.run_id,
            metrics=[
                Metric(key=name, value=float(value), timestamp=now, step=int(step))
                for step, name, value in long.itertuples(index=False)
            ],
        )
        mlflow.log_text(table.to_csv(index=False), "trainings.csv")
        names = "models/" + table["fold"].astype(str) + ".txt"
        files = [training["file"] for training in trainings]
        [mlflow.log_text(file, name) for file, name in zip(files, names, strict=True)]
        _log_training_rows(rows)
    return logged.info.run_id, mlflow_link(experiment_id, logged.info.run_id)


def _log_training_rows(rows: pd.DataFrame) -> None:
    """The rows a run's trainings learned from, as a Parquet file under `training/` of the
    active MLflow run, and beside it what they are: the tables read, the columns, the days."""
    import mlflow

    days = pd.to_datetime(rows["target_day"])  # a plain date, readable without BigQuery's types
    described = {
        "tables": TRAINING_TABLES,
        "columns": list(rows.columns),
        "first_target_day": str(days.min().date()),
        "last_target_day": str(days.max().date()),
        "rows": len(rows),
    }
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "rows.parquet"
        rows.assign(target_day=days).to_parquet(path, index=False)
        mlflow.log_artifact(str(path), "training")
    mlflow.log_text(json.dumps(described, indent=2), "training/set.json")


def log_backtest(
    *,
    run: Mapping,
    first_month: date,
    last_month: date,
    per_fold: pd.DataFrame,
    overall: dict[str, float],
    groups: pd.DataFrame,
    fidelity: dict[str, int],
) -> tuple[str, str]:
    """One MLflow run per model per backtest: the overall metrics, each metric per fold as a step
    (MLflow draws them over the folds), each per kind of day (`sky/clear/mae_share`), and the
    tables of folds and of groups. `run` is what the run recorded of itself. Returns MLflow's
    run ID and a link to it."""
    import mlflow
    from mlflow.entities import Metric

    mlflow.set_tracking_uri(tracking_uri())
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT)
    experiment_id = (
        experiment.experiment_id
        if experiment
        else client.create_experiment(EXPERIMENT, artifact_location=artifact_root())
    )
    run_id, model = run["run_id"], run["model"]
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"{model} {run_id[:8]}") as logged:
        mlflow.set_tags(
            {
                "dagster_run_id": run_id,
                "model": model,
                "mode": "backtest",
                "image_digest": os.environ.get("JTF_IMAGE_DIGEST", ""),
                **identity(run),
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
        mlflow.log_metrics(group_metrics(groups))
        now = int(time.time() * 1000)
        client.log_batch(
            logged.info.run_id,
            metrics=[
                Metric(key=f"fold_{name}", value=float(value), timestamp=now, step=step)
                for name in SCORES
                if name in per_fold
                for step, value in enumerate(per_fold[name])
            ],
        )
        mlflow.log_text(per_fold.to_csv(index=False), "folds.csv")
        mlflow.log_text(groups.to_csv(index=False), "groups.csv")
    return logged.info.run_id, mlflow_link(experiment_id, logged.info.run_id)


def log_benchmark(
    *, run: Mapping, per_day: pd.DataFrame, overall: dict[str, float]
) -> tuple[str, str]:
    """One MLflow run for a run set beside TEPCO's forecast: each side's miss overall, and the
    table per target day. The number of days is a metric of its own: a figure over four days is
    not one over forty. Returns MLflow's run ID and a link to it."""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri())
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(BENCHMARK)
    experiment_id = (
        experiment.experiment_id
        if experiment
        else client.create_experiment(BENCHMARK, artifact_location=artifact_root())
    )
    run_id, model = run["run_id"], run["model"]
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"{model} {run_id[:8]}") as logged:
        mlflow.set_tags(
            {
                "dagster_run_id": run_id,
                "model": model,
                "mode": str(run.get("mode", "")),
                "image_digest": os.environ.get("JTF_IMAGE_DIGEST", ""),
                **identity(run),
            }
        )
        mlflow.log_params(
            {
                "first_target_day": str(per_day["target_day"].min()),
                "last_target_day": str(per_day["target_day"].max()),
                "against": "TEPCO's next-day forecast, wind and solar together",
                "slots_scored": "daylight, with actuals",
            }
        )
        mlflow.log_metrics({name: float(value) for name, value in overall.items()})
        mlflow.log_text(per_day.to_csv(index=False), "days.csv")
    return logged.info.run_id, mlflow_link(experiment_id, logged.info.run_id)
