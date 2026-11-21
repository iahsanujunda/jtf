"""`make dagster-update`: no run sees the new SQL with the old asset graph, and Dagster is never
left with its sensors stopped on code that loaded."""

from pathlib import Path

import pytest

from jtf.orchestration.update import (
    Dagster,
    Refused,
    Sensor,
    may_write,
    update,
    write_instance_settings,
)

OLD, NEW = "a" * 40, "b" * 40
SENSORS = [Sensor("automation", "s1"), Sensor("raw_bucket_sensor", "s2")]


class FakeGit:
    def __init__(self, log, *, dagster=True, branch="main", dirty=False, new=NEW, **kwargs):
        changed, pull_fails = kwargs.get("changed", ()), kwargs.get("pull_fails", False)
        self.log, self.repo, self._dagster = log, Path("/checkout"), dagster
        self._branch, self._dirty, self._new = branch, dirty, new
        self._changed, self._pull_fails = set(changed), pull_fails

    def runs_dagster(self):
        return self._dagster

    def branch(self):
        return self._branch

    def dirty(self):
        return self._dirty

    def fetch(self):
        return OLD, self._new

    def changed(self, old, new):
        return self._changed

    def fast_forward(self, new):
        if self._pull_fails:
            raise RuntimeError("not a fast-forward")
        self.log.append(("pull", new))


class FakeDagster:
    def __init__(self, log, *, busy=(), load_error=False):
        self.log, self._busy, self._load_error = log, list(busy), load_error

    def running_sensors(self):
        return SENSORS

    def stop(self, sensor):
        self.log.append(("stop", sensor.name))

    def start(self, sensor):
        self.log.append(("start", sensor.name))

    def active_runs(self):
        runs = self._busy.pop(0) if self._busy else []
        self.log.append(("runs", len(runs)))
        return runs

    def reload(self):
        if self._load_error:
            raise RuntimeError("the code location failed to load: SyntaxError")
        self.log.append(("reload",))


def run(git, dagster, **kwargs):
    return update(git, dagster, sleep=lambda s: None, echo=lambda m: None, **kwargs)


def test_sensors_stop_runs_finish_then_pull_reload_and_start_in_that_order():
    """Each look at the runs comes a poll after the last: a tick under way when the sensors stop
    may still launch one."""
    log = []
    sleep = lambda s: log.append(("sleep", s))  # noqa: E731
    update(FakeGit(log), FakeDagster(log, busy=[["r1"], ["r1"]]), sleep=sleep, echo=lambda m: None)
    assert log == [
        ("stop", "automation"),
        ("stop", "raw_bucket_sensor"),
        ("sleep", 5),
        ("runs", 1),
        ("sleep", 5),
        ("runs", 1),
        ("sleep", 5),
        ("runs", 0),
        ("pull", NEW),
        ("reload",),
        ("start", "automation"),
        ("start", "raw_bucket_sensor"),
    ]


@pytest.mark.parametrize(
    "git",
    [
        {"dagster": False},
        {"branch": "pipeline-guards"},
        {"dirty": True},
        {"changed": {"uv.lock", "warehouse/dbt/models/dws/x.sql"}},
    ],
    ids=["not Dagster's checkout", "not on main", "uncommitted changes", "dependencies changed"],
)
def test_refused_before_anything_is_stopped_or_pulled(git):
    log = []
    with pytest.raises(Refused):
        run(FakeGit(log, **git), FakeDagster(log))
    assert log == []


def test_nothing_to_pull_still_reloads_with_the_sensors_stopped():
    """On 2026-11-19 the checkout was pulled by hand: it was up to date, and Dagster still ran
    the code it had loaded eight hours before."""
    log = []
    said = run(FakeGit(log, new=OLD), FakeDagster(log, busy=[["r1"]]))
    assert said.startswith("already up to date") and "reloaded" in said
    assert log == [
        ("stop", "automation"),
        ("stop", "raw_bucket_sensor"),
        ("runs", 1),
        ("runs", 0),
        ("reload",),
        ("start", "automation"),
        ("start", "raw_bucket_sensor"),
    ]


def test_runs_that_never_finish_leave_the_old_code_running_with_its_sensors():
    log = []
    with pytest.raises(Refused, match="still active"):
        run(FakeGit(log), FakeDagster(log, busy=[["r1"]] * 10), timeout=15, poll=5)
    assert ("pull", NEW) not in log
    assert log[-2:] == [("start", "automation"), ("start", "raw_bucket_sensor")]


def test_a_failed_pull_starts_the_sensors_again():
    log = []
    with pytest.raises(RuntimeError, match="fast-forward"):
        run(FakeGit(log, pull_fails=True), FakeDagster(log))
    assert log[-2:] == [("start", "automation"), ("start", "raw_bucket_sensor")]


def test_code_that_fails_to_load_keeps_the_sensors_stopped():
    log = []
    with pytest.raises(RuntimeError, match="sensors stay stopped"):
        run(FakeGit(log), FakeDagster(log, load_error=True))
    assert ("pull", NEW) in log
    assert not [e for e in log if e[0] == "start"]


def test_a_change_to_models_is_named_for_checking_in_the_data():
    log = []
    changed = {"warehouse/dbt/models/dws/dws_decision_slots.sql", "docs/phase1/progress.md"}
    out = run(FakeGit(log, changed=changed), FakeDagster(log))
    assert "warehouse/dbt/models/dws/dws_decision_slots.sql" in out


def test_the_api_calls_reach_dagster_as_graphql():
    """The GraphQL each call sends, against replies shaped like Dagster 1.13's."""
    sent = []

    def post(query, variables):
        sent.append((query, variables))
        if "sensorsOrError" in query:
            results = [
                {"name": "automation", "sensorState": {"id": "o::s1", "status": "RUNNING"}},
                {"name": "old", "sensorState": {"id": "o::s3", "status": "STOPPED"}},
            ]
            return {"data": {"sensorsOrError": {"results": results}}}
        if "runsOrError" in query:
            return {"data": {"runsOrError": {"results": [{"runId": "r1"}]}}}
        if "reloadRepositoryLocation" in query:
            entry = {"locationOrLoadError": {"__typename": "PythonError", "message": "boom"}}
            return {"data": {"reloadRepositoryLocation": entry}}
        return {"data": {}}

    dagster = Dagster("http://dagster/graphql", post=post)
    assert dagster.running_sensors() == [Sensor("automation", "o::s1")]
    dagster.stop(Sensor("automation", "o::s1"))
    assert sent[-1][1] == {"id": "o::s1"}
    assert dagster.active_runs() == ["r1"]
    assert "STARTED" in sent[-1][1]["statuses"] and "QUEUED" in sent[-1][1]["statuses"]
    with pytest.raises(RuntimeError, match="boom"):
        dagster.reload()


def test_what_new_code_needs_is_prepared_after_the_pull_and_before_the_reload():
    log = []
    run(FakeGit(log), FakeDagster(log), prepare=lambda: log.append(("prepare",)))

    done = [step[0] for step in log]
    assert done.index("pull") < done.index("prepare") < done.index("reload")
    assert done.index("reload") < done.index("start")


def test_a_failed_preparation_keeps_the_sensors_stopped_and_nothing_reloads():
    log = []

    def no_column():
        raise RuntimeError("the table could not be extended")

    with pytest.raises(RuntimeError, match="sensors stay stopped"):
        run(FakeGit(log), FakeDagster(log), prepare=no_column)

    done = [step[0] for step in log]
    assert "pull" in done and "reload" not in done and "start" not in done


@pytest.mark.parametrize(
    "settings, branch, dirty, refused",
    [
        ({"JTF_ENV": "prod"}, "main", False, None),
        ({"JTF_ENV": "prod"}, "a-branch", False, "not main"),
        ({"JTF_ENV": "prod"}, "main", True, "uncommitted"),
        ({"JTF_ENV": "dev"}, "a-branch", True, None),
        ({}, "a-branch", True, None),
    ],
    ids=["prod on clean main", "prod on a branch", "prod with changes", "dev", "unset is dev"],
)
def test_prod_is_written_only_from_a_clean_checkout_of_main(settings, branch, dirty, refused):
    git = FakeGit([], branch=branch, dirty=dirty)
    if refused is None:
        may_write(git, settings)
    else:
        with pytest.raises(Refused, match=refused):
            may_write(git, settings)


def test_dagsters_own_settings_are_written_from_the_repository_once(tmp_path):
    wanted = "retention:\n  sensor:\n    purge_after_days: 1\n"
    (tmp_path / "deploy" / "dagster").mkdir(parents=True)
    (tmp_path / "deploy" / "dagster" / "dagster.local.yaml").write_text(wanted)

    assert write_instance_settings(tmp_path) is True
    assert (tmp_path / ".dagster" / "dagster.yaml").read_text() == wanted
    assert write_instance_settings(tmp_path) is False  # as it is: nothing to write
    (tmp_path / ".dagster" / "dagster.yaml").write_text("edited by hand")
    assert write_instance_settings(tmp_path) is True
    assert (tmp_path / ".dagster" / "dagster.yaml").read_text() == wanted


def test_the_settings_in_the_repository_are_ones_dagster_accepts(tmp_path):
    import dagster as dg
    from dagster._core.definitions.run_request import InstigatorType

    repo = Path(__file__).resolve().parents[2]
    (tmp_path / "deploy" / "dagster").mkdir(parents=True)
    source = repo / "deploy" / "dagster" / "dagster.local.yaml"
    (tmp_path / "deploy" / "dagster" / "dagster.local.yaml").write_text(source.read_text())
    write_instance_settings(tmp_path)

    with dg.DagsterInstance.from_config(str(tmp_path / ".dagster")) as instance:
        kept = instance.get_tick_retention_settings(InstigatorType.SENSOR)
    assert {status.value: days for status, days in kept.items()}["SKIPPED"] == 1
