import json
from datetime import UTC, datetime

import httpx

from jtf.ingest.fetch import Healthcheck, run_group
from jtf.storage import LocalStore

FIXED_NOW = datetime(2026, 11, 15, 10, 0, 5, tzinfo=UTC)  # 19:00:05 JST


def source_client(fail: set[str] = frozenset()) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name in fail:
            return httpx.Response(404)
        return httpx.Response(
            200, content=f"body of {name}".encode(), headers={"Set-Cookie": "lb=1", "ETag": '"e"'}
        )

    return httpx.Client(transport=httpx.MockTransport(handler))


def ping_recorder():
    pings = []

    def handler(request: httpx.Request) -> httpx.Response:
        pings.append(request.url.path)
        return httpx.Response(200)

    return pings, httpx.Client(transport=httpx.MockTransport(handler))


def test_evening_group_stores_files_and_sidecars(tmp_path):
    store = LocalStore(tmp_path)
    pings, ping_client = ping_recorder()
    result = run_group(
        "evening",
        store=store,
        client=source_client(),
        healthcheck=Healthcheck("https://hc.example/abc", ping_client),
        now=lambda: FIXED_NOW,
        image_digest="sha256:test",
    )

    assert result.ok
    path = "tepco_forecast_next_day/2026-11-15/20261115T100005Z_AREA_ONCE_YOSOKU.csv"
    assert store.get(path) == b"body of AREA_ONCE_YOSOKU.csv"
    meta = json.loads(store.get(path + ".meta.json"))
    assert meta["fetched_at"] == "2026-11-15T10:00:05+00:00"
    assert meta["extraction_date"] == "2026-11-15"
    assert meta["capture_group"] == "evening"
    assert meta["image_digest"] == "sha256:test"
    assert "set-cookie" not in {k.lower() for k in meta["response_headers"]}
    assert pings == ["/abc/start", "/abc"]


def test_extraction_date_is_the_japan_date(tmp_path):
    store = LocalStore(tmp_path)
    late_utc = datetime(2026, 11, 15, 23, 45, tzinfo=UTC)  # 08:45 JST on the 16th
    result = run_group("morning", store=store, client=source_client(), now=lambda: late_utc)
    assert result.ok
    assert all("/2026-11-16/" in s for s in result.stored)


def test_one_failure_fails_the_group_but_keeps_the_rest(tmp_path):
    store = LocalStore(tmp_path)
    pings, ping_client = ping_recorder()
    result = run_group(
        "evening",
        store=store,
        client=source_client(fail={"AREA_ONCE_BGKEI.csv"}),
        healthcheck=Healthcheck("https://hc.example/abc", ping_client),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
    )

    assert not result.ok
    assert len(result.stored) == 1
    assert "AREA_ONCE_BGKEI.csv" in result.failures[0]
    assert pings == ["/abc/start", "/abc/fail"]


def test_server_errors_are_retried(tmp_path):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(503) if len(calls) < 2 else httpx.Response(200, content=b"ok")

    result = run_group(
        "jepx",
        store=LocalStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
    )
    assert result.ok
    assert len(calls) == 2
