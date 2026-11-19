"""What MLflow is told about a run (jtf.ml.tracking): the run's own commit and fingerprints,
links to its code and to the run in Dagster, and for a trained model the files it leaves: each
month's model, and the rows they learned from."""

import json
from datetime import date
from pathlib import Path

import dagster as dg
import mlflow
import pandas as pd
import pytest

from jtf.ml import tracking
from jtf.ml.backtest import MEAN_7D_RANGE
from jtf.ml.replay import run
from jtf.orchestration import ml
from jtf.orchestration.resources import Datasets
from test_trained import CODE, NOW, FakeWarehouse, world

PUBLISHED = "https://example.org/owner/repo"
RUN = {
    "run_id": "0a1b2c3d-run",
    "model": MEAN_7D_RANGE,
    "git_commit": "abc1234def",
    "tree_dirty": False,
    "git_url": PUBLISHED,
    "inputs_fingerprint": "111",
    "training_fingerprint": "222",
    "outputs_fingerprint": "333",
}


@pytest.fixture
def store(tmp_path, monkeypatch):
    """An MLflow store of the test's own, where the repository's would be."""
    monkeypatch.setattr(tracking, "REPO", tmp_path)
    for name in ("MLFLOW_TRACKING_URI", "JTF_DAGSTER_URL", "JTF_MLFLOW_URL"):
        monkeypatch.delenv(name, raising=False)
    return mlflow.MlflowClient(tracking_uri=tracking.tracking_uri())


def files(client: mlflow.MlflowClient, run_id: str, folder: str | None = None) -> list[str]:
    return sorted(item.path for item in client.list_artifacts(run_id, folder))


@pytest.mark.parametrize(
    "remote",
    [
        "git@example.org:owner/repo.git",
        "ssh://git@example.org/owner/repo.git",
        "https://example.org/owner/repo.git",
        "https://someone:secret@example.org/owner/repo",
    ],
)
def test_a_remote_is_read_as_the_address_of_its_page_without_what_logs_in(remote):
    assert tracking.web_url(remote) == PUBLISHED


def test_an_entry_names_its_runs_own_commit_and_fingerprints_and_links_to_both():
    tags = tracking.identity(RUN)

    assert tags["git_commit"] == tags["mlflow.source.git.commit"] == "abc1234def"
    assert tags["mlflow.source.name"] == tags["mlflow.source.git.repoURL"] == PUBLISHED
    assert (tags["inputs_fingerprint"], tags["training_fingerprint"]) == ("111", "222")
    assert tags["outputs_fingerprint"] == "333" and tags["git_dirty"] == "false"
    note = tags["mlflow.note.content"]
    assert f"[`abc1234`]({PUBLISHED}/tree/abc1234def)" in note
    assert "[`0a1b2c3d`](http://localhost:3000/runs/0a1b2c3d-run)" in note
    assert "Training fingerprint: `222`" in note and "uncommitted" not in note


def test_an_entry_says_when_the_commit_is_not_all_of_the_code_or_has_no_page():
    dirty = tracking.identity(RUN | {"tree_dirty": True})
    unpublished = tracking.identity(RUN | {"git_url": None, "training_fingerprint": None})

    assert dirty["git_dirty"] == "true" and "uncommitted changes" in dirty["mlflow.note.content"]
    assert "Code: `abc1234`" in unpublished["mlflow.note.content"]
    assert "mlflow.source.name" not in unpublished and "training_fingerprint" not in unpublished
    assert "Training fingerprint" not in unpublished["mlflow.note.content"]


def test_the_uis_are_linked_where_the_settings_say_they_are(monkeypatch):
    monkeypatch.setenv("JTF_DAGSTER_URL", "https://dagster.example.org/")
    monkeypatch.setenv("JTF_MLFLOW_URL", "https://mlflow.example.org")

    assert tracking.links(RUN)["dagster_run"] == "https://dagster.example.org/runs/0a1b2c3d-run"
    assert tracking.mlflow_link("7", "e1") == "https://mlflow.example.org/#/experiments/7/runs/e1"


def test_a_trained_runs_entry_keeps_each_months_model_and_the_rows_they_learned_from(store):
    warehouse = FakeWarehouse(world())
    days = {"first_day": date(2026, 2, 1), "last_day": date(2026, 3, 31)}
    record = run(
        warehouse, run_id="r1", mode="backtest", model=MEAN_7D_RANGE, code=CODE, now=NOW, **days
    )

    logged, link = tracking.log_trainings(
        run=record | {"git_url": PUBLISHED},
        trainings=record["trainings"],
        rows=record["training_rows"],
    )

    entry = store.get_run(logged)
    assert link == f"http://localhost:5000/#/experiments/{entry.info.experiment_id}/runs/{logged}"
    assert entry.data.tags["training_fingerprint"] == record["training_fingerprint"]
    assert entry.data.tags["mlflow.source.git.commit"] == "abc123"
    assert files(store, logged, "models") == ["models/2026-02-01.txt", "models/2026-03-01.txt"]
    assert files(store, logged, "training") == ["training/rows.parquet", "training/set.json"]
    folder = mlflow.artifacts.download_artifacts(
        run_id=logged, artifact_path="training", tracking_uri=tracking.tracking_uri()
    )
    kept = pd.read_parquet(f"{folder}/rows.parquet")
    described = json.loads(Path(folder, "set.json").read_text())
    assert len(kept) == len(record["training_rows"]) == described["rows"] == 58
    assert list(kept["label_mw"]) == list(record["training_rows"]["label_mw"])
    assert described["columns"] == list(record["training_rows"].columns)
    assert (described["first_target_day"], described["last_target_day"]) == (
        "2026-01-01",
        "2026-02-27",
    )
    assert described["tables"] == ["dws_solar_features", "ads_ml_label_solar", "dim_daylight"]
    trainings = pd.read_csv(
        mlflow.artifacts.download_artifacts(
            run_id=logged, artifact_path="trainings.csv", tracking_uri=tracking.tracking_uri()
        ),
        dtype=str,
    )
    assert list(trainings["training_fingerprint"]) == [
        t["training_fingerprint"] for t in record["trainings"]
    ]


def test_a_backtests_entry_carries_the_runs_record_not_the_code_checked_out_when_scored(store):
    per_fold = pd.DataFrame({"fold": [date(2026, 2, 1)], "mae_share": [0.05]})
    groups = pd.DataFrame({"grouping": ["sky"], "group": ["clear"], "mae_share": [0.03]})

    logged, link = tracking.log_backtest(
        run=RUN,
        first_month=date(2026, 2, 1),
        last_month=date(2026, 2, 1),
        per_fold=per_fold,
        overall={"mae_share": 0.05},
        groups=groups,
        fidelity={"exact": 1, "approximate": 0},
    )

    entry = store.get_run(logged)
    assert entry.data.tags["git_commit"] == "abc1234def"  # the run's, whatever is checked out
    assert entry.data.tags["dagster_run_id"] == "0a1b2c3d-run" and logged in link
    assert entry.data.tags["outputs_fingerprint"] == "333"
    assert entry.data.metrics["sky/clear/mae_share"] == 0.03


class StandIn(Datasets):
    """The resource the assets take, handing out a warehouse in memory."""

    def backtest(self):
        return WAREHOUSE


WAREHOUSE = None


def test_a_run_in_dagster_shows_what_it_was_made_from_and_links_to_its_entries(store, monkeypatch):
    global WAREHOUSE
    WAREHOUSE = FakeWarehouse(world())
    code = {"git_commit": "abc1234def", "git_dirty": "false", "git_url": PUBLISHED}
    monkeypatch.setattr(ml, "run_code", lambda: code | {"image_digest": ""})
    config = {"model": MEAN_7D_RANGE, "first_test_month": "2026-02", "last_test_month": "2026-03"}

    result = dg.materialize(
        [ml.solar_predictions],
        resources={"datasets": StandIn(project="p", location="l", prefix="x")},
        run_config={"ops": {ml.solar_predictions.op.name: {"config": config}}},
        instance=dg.DagsterInstance.ephemeral(),
    )

    (event,) = result.get_asset_materialization_events()
    shown = {k: v.value for k, v in event.materialization.metadata.items()}
    assert shown["code"] == f"{PUBLISHED}/tree/abc1234def" and shown["git_url"] == PUBLISHED
    assert shown["git_commit"] == "abc1234def" and shown["tree_dirty"] is False
    assert shown["training_fingerprint"] == WAREHOUSE.runs[0].iloc[0]["training_fingerprint"]
    assert shown["inputs_fingerprint"] and shown["outputs_fingerprint"]
    entry = store.get_run(shown["trainings_mlflow_run_id"])
    assert shown["trainings_in_mlflow"].endswith(f"/runs/{entry.info.run_id}")
    assert entry.data.tags["dagster_run_id"] == shown["run_id"] == result.run_id
    assert f"/runs/{result.run_id})" in entry.data.tags["mlflow.note.content"]
    assert files(store, entry.info.run_id, "training") == [
        "training/rows.parquet",
        "training/set.json",
    ]
