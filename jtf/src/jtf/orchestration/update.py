"""Bring the checkout Dagster runs from up to `main`, as one step (`make dagster-update`).

Dagster reads its asset graph when the code location loads, but dbt reads the SQL from disk at every
run, so a pull without a reload runs new SQL on the old graph. This stops the sensors launching
runs, waits for the running ones, pulls, reloads, and starts the sensors again: no run sees half of
each. It is for the laptop; in prod the image digest pins the code and its graph together.
"""

import json
import subprocess
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

LOCATION = "jtf.orchestration.definitions"
REPOSITORY = "__repository__"
ACTIVE = ["QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING"]
# A change to these needs `make setup` and a restart: a reload keeps the installed packages.
DEPENDENCIES = {"pyproject.toml", "uv.lock", "jtf/pyproject.toml"}


class Refused(Exception):
    """Nothing was pulled; the message says why and what to do instead."""


@dataclass(frozen=True)
class Sensor:
    name: str
    state_id: str


class Dagster:
    """The few calls the update needs, over Dagster's GraphQL API."""

    def __init__(self, url: str, post: Callable[[str, dict], dict] | None = None):
        self.url = url
        self._post = post or self._http

    def _http(self, query: str, variables: dict) -> dict:
        body = json.dumps({"query": query, "variables": variables}).encode()
        request = urllib.request.Request(
            self.url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)

    def _call(self, query: str, **variables) -> dict:
        reply = self._post(query, variables)
        if reply.get("errors"):
            raise RuntimeError(f"Dagster's API: {reply['errors']}")
        return reply["data"]

    def running_sensors(self) -> list[Sensor]:
        data = self._call(
            """query($location: String!, $repository: String!) {
              sensorsOrError(repositorySelector: {
                repositoryLocationName: $location, repositoryName: $repository}) {
                ... on Sensors { results { name sensorState { id status } } }
              }
            }""",
            location=LOCATION,
            repository=REPOSITORY,
        )
        return [
            Sensor(s["name"], s["sensorState"]["id"])
            for s in data["sensorsOrError"]["results"]
            if s["sensorState"]["status"] == "RUNNING"
        ]

    def stop(self, sensor: Sensor) -> None:
        self._call(
            "mutation($id: String!) { stopSensor(id: $id) { __typename } }", id=sensor.state_id
        )

    def start(self, sensor: Sensor) -> None:
        self._call(
            """mutation($location: String!, $repository: String!, $name: String!) {
              startSensor(sensorSelector: {repositoryLocationName: $location,
                repositoryName: $repository, sensorName: $name}) { __typename }
            }""",
            location=LOCATION,
            repository=REPOSITORY,
            name=sensor.name,
        )

    def active_runs(self) -> list[str]:
        data = self._call(
            """query($statuses: [RunStatus!]) {
              runsOrError(filter: {statuses: $statuses}) { ... on Runs { results { runId } } }
            }""",
            statuses=ACTIVE,
        )
        return [r["runId"] for r in data["runsOrError"]["results"]]

    def reload(self) -> None:
        data = self._call(
            """mutation($location: String!) {
              reloadRepositoryLocation(repositoryLocationName: $location) {
                __typename
                ... on WorkspaceLocationEntry { locationOrLoadError {
                  __typename ... on PythonError { message } } }
                ... on PythonError { message }
              }
            }""",
            location=LOCATION,
        )["reloadRepositoryLocation"]
        loaded = data.get("locationOrLoadError") or data
        if loaded["__typename"] != "RepositoryLocation":
            raise RuntimeError(f"the code location failed to load: {loaded.get('message', '')}")


class Git:
    """The checkout Dagster runs from."""

    def __init__(self, repo: Path):
        self.repo = repo

    def _git(self, *args: str) -> str:
        out = subprocess.run(
            ["git", "-C", str(self.repo), *args], check=True, capture_output=True, text=True
        )
        return out.stdout.strip()

    def runs_dagster(self) -> bool:
        return (self.repo / ".dagster").is_dir()

    def branch(self) -> str:
        return self._git("branch", "--show-current")

    def dirty(self) -> bool:
        return bool(self._git("status", "--porcelain", "--untracked-files=no"))

    def fetch(self) -> tuple[str, str]:
        """HEAD and origin/main after fetching it."""
        self._git("fetch", "--quiet", "origin", "main")
        return self._git("rev-parse", "HEAD"), self._git("rev-parse", "origin/main")

    def changed(self, old: str, new: str) -> set[str]:
        return set(self._git("diff", "--name-only", old, new).splitlines())

    def fast_forward(self, new: str) -> None:
        self._git("merge", "--ff-only", "--quiet", new)


def update(
    git: Git,
    dagster: Dagster,
    *,
    timeout: float = 900,
    poll: float = 5,
    sleep: Callable[[float], None] = time.sleep,
    echo: Callable[[str], None] = print,
) -> str:
    """Pull `main` into Dagster's checkout and reload, with no run in between. Returns what
    changed; raises Refused if nothing was pulled, RuntimeError if the new code failed to load.
    With nothing to pull it still reloads: the checkout may have moved without one (a pull by
    hand), and Dagster can't say which commit it loaded."""
    if not git.runs_dagster():
        raise Refused(f"{git.repo} has no .dagster/: it is not the checkout Dagster runs from")
    if git.branch() != "main":
        raise Refused(f"{git.repo} is on {git.branch()!r}, not main")
    if git.dirty():
        raise Refused(f"{git.repo} has uncommitted changes")
    old, new = git.fetch()
    changed = git.changed(old, new) if old != new else set()
    if changed & DEPENDENCIES:
        raise Refused(
            f"dependencies changed ({', '.join(sorted(changed & DEPENDENCIES))}): stop "
            "`make dagster`, then `git pull`, `make setup` and `make dagster`"
        )

    sensors = dagster.running_sensors()
    for sensor in sensors:
        dagster.stop(sensor)
    echo(f"stopped {', '.join(s.name for s in sensors) or 'no sensors'}; waiting for runs")
    try:
        waited = 0.0
        while True:
            # The first look comes a poll after the stop: a tick under way may still launch a run
            sleep(poll)
            waited += poll
            if not (runs := dagster.active_runs()):
                break
            if waited >= timeout:
                raise Refused(f"{len(runs)} runs still active after {timeout:.0f} s")
        if old != new:
            git.fast_forward(new)
    except BaseException:
        for sensor in sensors:
            dagster.start(sensor)
        raise
    try:
        dagster.reload()
    except Exception as e:
        raise RuntimeError(
            f"pulled {new[:7]}, but {e}. The sensors stay stopped: fix the code, reload, then "
            "start them in the UI (Automation)"
        ) from e
    for sensor in sensors:
        dagster.start(sensor)
    if old == new:
        return "already up to date; reloaded, in case the checkout moved without a reload"
    models = sorted(f for f in changed if f.startswith("warehouse/dbt/models/"))
    note = f"; models changed, check in the data that they were rebuilt: {models}" if models else ""
    return f"{old[:7]} -> {new[:7]}, {len(changed)} files, reloaded{note}"
