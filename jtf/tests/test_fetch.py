import json
from datetime import UTC, datetime

import httpx
import pytest

from jtf.ingest.fetch import Healthcheck, run_source, stored_files
from jtf.ingest.sources import get_source
from jtf.storage import LocalStore

FIXED_NOW = datetime(2026, 11, 15, 10, 0, 5, tzinfo=UTC)  # 19:00:05 JST


def source_client(fail: set[str] = frozenset(), seen: list | None = None) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if seen is not None:
            seen.append((name, request.headers.get("if-modified-since")))
        if name in fail:
            return httpx.Response(404)
        return httpx.Response(
            200,
            content=f"body of {name}".encode(),
            headers={"Set-Cookie": "lb=1", "Last-Modified": f"LM of {name}"},
        )

    return httpx.Client(transport=httpx.MockTransport(handler))


def ping_recorder():
    pings = []

    def handler(request: httpx.Request) -> httpx.Response:
        pings.append((request.url.path, request.url.params.get("create")))
        return httpx.Response(200)

    return pings, httpx.Client(transport=httpx.MockTransport(handler))


def test_daily_stores_file_and_sidecar_and_pings_its_slug(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("tepco_area_forecast_next_day")
    pings, ping_client = ping_recorder()
    result = run_source(
        source,
        store=store,
        client=source_client(),
        healthcheck=Healthcheck.for_source("KEY", source, "daily", ping_client),
        now=lambda: FIXED_NOW,
        image_digest="sha256:test",
    )

    assert result.ok
    path = "tepco_area_forecast_next_day/2026-11-15/20261115T100005Z_AREA_ONCE_YOSOKU.csv"
    assert store.get(path) == b"body of AREA_ONCE_YOSOKU.csv"
    meta = json.loads(store.get(path + ".meta.json"))
    assert meta["source"] == "tepco_area_forecast_next_day"
    assert meta["load_type"] == "daily"
    assert meta["fetched_at"] == "2026-11-15T10:00:05+00:00"
    assert meta["extraction_date"] == "2026-11-15"
    assert meta["image_digest"] == "sha256:test"
    assert "set-cookie" not in {k.lower() for k in meta["response_headers"]}
    slug = "/KEY/jtf-tepco-area-forecast-next-day"
    assert pings == [(f"{slug}/start", "1"), (slug, "1")]


def test_extraction_date_is_the_japan_date(tmp_path):
    late_utc = datetime(2026, 11, 15, 23, 45, tzinfo=UTC)  # 08:45 JST on the 16th
    result = run_source(
        get_source("tepco_actuals"),
        store=LocalStore(tmp_path),
        client=source_client(),
        now=lambda: late_utc,
        sleep=lambda _: None,
    )
    assert result.ok
    assert all("/2026-11-16/" in s for s in result.stored)


def test_a_failed_file_fails_the_source_and_pings_fail(tmp_path):
    source = get_source("tepco_actuals")
    pings, ping_client = ping_recorder()
    result = run_source(
        source,
        store=LocalStore(tmp_path),
        client=source_client(fail={"eria_jukyu_202610_03.csv"}),
        healthcheck=Healthcheck.for_source("KEY", source, "daily", ping_client),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
    )

    assert not result.ok
    assert len(result.stored) == 1
    assert "eria_jukyu_202610_03.csv" in result.failures[0]
    assert [p for p, _ in pings] == ["/KEY/jtf-tepco-actuals/start", "/KEY/jtf-tepco-actuals/fail"]


def test_connect_failures_are_retried_up_to_the_policy(tmp_path):
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) < 5:
            raise httpx.ConnectTimeout("handshake timed out")
        return httpx.Response(200, content=b"{}")

    result = run_source(
        get_source("openmeteo_live"),
        store=LocalStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
    )
    assert result.ok
    assert len(calls) == 5


def test_initial_load_skips_what_is_already_stored_so_it_can_resume(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("jepx_spot")
    first = run_source(
        source,
        mode="initial",
        store=store,
        client=source_client(),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
        max_calls=None,
    )
    assert first.ok and len(first.stored) == 10

    seen = []
    again = run_source(
        source,
        mode="initial",
        store=store,
        client=source_client(seen=seen),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
    )
    assert again.ok
    assert len(again.skipped) == 10 and not again.stored
    assert seen == []
    meta_path = next(p for p in store.list("jepx_spot/") if p.endswith(".meta.json"))
    assert json.loads(store.get(meta_path))["load_type"] == "initial"


def test_recapture_sends_last_modified_and_stores_only_rewrites(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("tepco_actuals")
    run_source(
        source, store=store, client=source_client(), now=lambda: FIXED_NOW, sleep=lambda _: None
    )

    seen = []

    def handler(request):
        name = request.url.path.rsplit("/", 1)[-1]
        seen.append((name, request.headers.get("if-modified-since")))
        if name == "eria_jukyu_202610_03.csv":
            return httpx.Response(304)
        return httpx.Response(200, content=b"rewritten", headers={"Last-Modified": "new"})

    later = datetime(2026, 12, 1, 0, 30, tzinfo=UTC)
    result = run_source(
        source,
        mode="recapture",
        store=store,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: later,
        sleep=lambda _: None,
    )

    assert result.ok
    sent = dict(seen)
    assert sent["eria_jukyu_202610_03.csv"] == "LM of eria_jukyu_202610_03.csv"
    assert sent["eria_jukyu_202402_03.csv"] is None  # never captured before: plain request
    assert result.unchanged == ["eria_jukyu_202610_03.csv"]
    assert len(result.stored) == len(seen) - 1
    assert len(stored_files(store, "tepco_actuals")["eria_jukyu_202611_03.csv"]) == 2


def open_meteo_client(
    cost: float, statuses: dict[int, int] | None = None, calls: list | None = None
):
    def handler(request):
        if calls is not None:
            calls.append(1)
        status = (statuses or {}).get(len(calls or []), 200)
        return httpx.Response(status, content=b"{}", headers={"X-RateLimit-Cost": str(cost)})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_call_budget_stops_cleanly_and_a_later_run_resumes(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("openmeteo_previous_runs")
    waits = []
    first = run_source(
        source,
        mode="initial",
        store=store,
        client=open_meteo_client(104.0),
        now=lambda: FIXED_NOW,
        sleep=waits.append,
        max_calls=500,
    )
    assert first.ok
    assert len(first.stored) == 4 and first.stopped
    assert waits[0] == pytest.approx(104 * 60 / 600 * 1.1)  # spreads the weight over a minute

    second = run_source(
        source,
        mode="initial",
        store=store,
        client=open_meteo_client(104.0),
        now=lambda: FIXED_NOW,
        sleep=lambda _: None,
        max_calls=10_000,
    )
    assert len(second.skipped) == 4
    assert len(second.stored) == 35 - 4 and not second.stopped


def test_quota_refusal_stops_without_failing(tmp_path):
    calls = []
    result = run_source(
        get_source("openmeteo_single_runs"),
        mode="initial",
        store=LocalStore(tmp_path),
        client=open_meteo_client(9.0, statuses={3: 429}, calls=calls),
        now=lambda: datetime(2026, 4, 10, tzinfo=UTC),
        sleep=lambda _: None,
    )
    assert result.ok
    assert len(result.stored) == 2 and "429" in result.stopped


def test_initial_loads_do_not_ping_and_recapture_has_its_own_check():
    source = get_source("tepco_actuals")
    assert Healthcheck.for_source("KEY", source, "initial").url is None
    assert Healthcheck.for_source("KEY", source, "recapture").url.endswith(
        "/KEY/jtf-tepco-actuals-recapture"
    )
    assert Healthcheck.for_source(None, source, "daily").url is None


def test_a_mode_a_source_lacks_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        run_source(get_source("openmeteo_single_runs"), store=LocalStore(tmp_path))
