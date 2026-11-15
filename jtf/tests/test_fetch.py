import json
from datetime import UTC, datetime

import httpx

from jtf.ingest.fetch import Healthcheck, run_source
from jtf.ingest.sources import get_source
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
        pings.append((request.url.path, request.url.params.get("create")))
        return httpx.Response(200)

    return pings, httpx.Client(transport=httpx.MockTransport(handler))


def test_source_stores_file_and_sidecar_and_pings_its_slug(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("tepco_area_forecast_next_day")
    pings, ping_client = ping_recorder()
    result = run_source(
        source,
        store=store,
        client=source_client(),
        healthcheck=Healthcheck.for_source("KEY", source, ping_client),
        now=lambda: FIXED_NOW,
        image_digest="sha256:test",
    )

    assert result.ok
    path = "tepco_area_forecast_next_day/2026-11-15/20261115T100005Z_AREA_ONCE_YOSOKU.csv"
    assert store.get(path) == b"body of AREA_ONCE_YOSOKU.csv"
    meta = json.loads(store.get(path + ".meta.json"))
    assert meta["source"] == "tepco_area_forecast_next_day"
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
    )
    assert result.ok
    assert all("/2026-11-16/" in s for s in result.stored)


def test_a_failed_file_fails_the_source_and_pings_fail(tmp_path):
    source = get_source("tepco_actuals")
    early = datetime(2027, 1, 2, 23, 45, tzinfo=UTC)  # day 3 in Japan: two monthly files
    pings, ping_client = ping_recorder()
    result = run_source(
        source,
        store=LocalStore(tmp_path),
        client=source_client(fail={"eria_jukyu_202612_03.csv"}),
        healthcheck=Healthcheck.for_source("KEY", source, ping_client),
        now=lambda: early,
        sleep=lambda _: None,
    )

    assert not result.ok
    assert len(result.stored) == 1
    assert "eria_jukyu_202612_03.csv" in result.failures[0]
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


def test_no_ping_key_means_no_pings():
    assert Healthcheck.for_source(None, get_source("jepx_spot")).url is None
