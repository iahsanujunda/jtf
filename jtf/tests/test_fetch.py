import json
from datetime import UTC, datetime, timedelta

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


def test_a_daily_capture_of_unwatched_files_never_lists_what_is_stored(tmp_path):
    class NoListing(LocalStore):
        def list(self, prefix):
            raise AssertionError(f"listed {prefix}")

    source = get_source("tepco_area_forecast_next_day")
    store = NoListing(tmp_path)
    assert run_source(source, store=store, client=source_client(), now=lambda: FIXED_NOW).ok


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


def test_a_run_missing_from_the_archive_is_stored_as_a_gap_once(tmp_path):
    store = LocalStore(tmp_path)
    source = get_source("openmeteo_single_runs")
    missing = b'{"error":true,"reason":"The requested model run is not available."}'

    def handler(request):
        if request.url.params["models"] == "jma_msm":
            return httpx.Response(400, content=missing, headers={"X-RateLimit-Cost": "9"})
        return httpx.Response(200, content=b"{}", headers={"X-RateLimit-Cost": "9"})

    first = run_source(
        source,
        mode="initial",
        store=store,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: datetime(2026, 4, 4, 3, tzinfo=UTC),
        sleep=lambda _: None,
    )
    assert first.ok
    assert first.gaps == ["jma_msm_2026-04-02T18Z.json", "jma_msm_2026-04-02T21Z.json"]
    assert len(first.stored) == 4
    gap_meta = next(
        p
        for p in store.list("openmeteo_single_runs/")
        if "jma_msm" in p and p.endswith(".meta.json")
    )
    meta = json.loads(store.get(gap_meta))
    assert meta["gap"] is True and meta["http_status"] == 400

    again = run_source(
        source,
        mode="initial",
        store=store,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: datetime(2026, 4, 4, 3, tzinfo=UTC),
        sleep=lambda _: None,
    )
    assert len(again.skipped) == 6 and not again.gaps


def test_other_client_errors_still_fail(tmp_path):
    def handler(request):
        return httpx.Response(400, content=b'{"error":true,"reason":"Cannot initialize"}')

    result = run_source(
        get_source("openmeteo_single_runs"),
        mode="initial",
        store=LocalStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: datetime(2026, 4, 4, 3, tzinfo=UTC),
        sleep=lambda _: None,
    )
    assert not result.ok and len(result.failures) == 6


# ---------------------------------------------------------------- an index page (FIT/FIP)

FIT_INDEX = """<html><body>
<a href="https://www.fit-portal.go.jp/servlet/servlet.FileDownload?file=idA2603">A&#34920;&#12288;都道府県別認定・導入量（２０２６年３月末時点）</a>
<a href="https://www.fit-portal.go.jp/servlet/servlet.FileDownload?file=idB2603">B&#34920;&#12288;市町村別認定・導入量（２０２６年３月末時点）</a>
<a href="https://www.fit-portal.go.jp/servlet/servlet.FileDownload?file=idA1712">A&#34920;&#12288;都道府県別認定・導入量（２０１７年１２月末時点）</a>
</body></html>""".encode()


FIT_LAST_MODIFIED = "Tue, 18 Aug 2026 04:42:27 GMT"


def fit_client(
    page: bytes = FIT_INDEX,
    seen: list | None = None,
    last_modified: dict[str, str | None] | None = None,
) -> httpx.Client:
    """The page, and each release file with its Last-Modified (FIT_LAST_MODIFIED unless given)."""

    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.params.get("file") or request.url.path.rsplit("/", 1)[-1]
        if seen is not None:
            seen.append(name if request.method == "GET" else f"{request.method} {name}")
        if name == "PublicInfoSummary":
            return httpx.Response(200, content=page)
        modified = (last_modified or {}).get(name, FIT_LAST_MODIFIED)
        headers = {"Last-Modified": modified} if modified else {}
        body = b"" if request.method == "HEAD" else f"workbook {name} of {modified}".encode()
        return httpx.Response(200, content=body, headers=headers)

    return httpx.Client(transport=httpx.MockTransport(handler))


def fit_run(store, mode="daily", days_later=0, **kwargs):
    return run_source(
        get_source("meti_fit_capacity"),
        mode=mode,
        store=store,
        now=lambda: FIXED_NOW + timedelta(days=days_later),
        sleep=lambda _: None,
        **kwargs,
    )


def test_daily_stores_the_page_then_checks_each_held_release_by_its_headers(tmp_path):
    store = LocalStore(tmp_path)
    first = fit_run(store, client=fit_client())
    assert first.ok
    # The 2017-12 link is an .xls release, which is left out.
    assert sorted(stored_files(store, "meti_fit_capacity")) == [
        "A_pref202603.xlsx",
        "B_city202603.xlsx",
        "PublicInfoSummary.html",
    ]
    (a_path,) = stored_files(store, "meti_fit_capacity")["A_pref202603.xlsx"]
    assert store.get(a_path) == f"workbook idA2603 of {FIT_LAST_MODIFIED}".encode()

    seen = []
    again = fit_run(store, days_later=1, client=fit_client(seen=seen))

    # The page every day; a release it holds, only its headers, and nothing stored for it.
    assert again.ok and seen == ["PublicInfoSummary", "HEAD idA2603", "HEAD idB2603"]
    assert sorted(again.unchanged) == ["A_pref202603.xlsx", "B_city202603.xlsx"]
    assert len(stored_files(store, "meti_fit_capacity")["A_pref202603.xlsx"]) == 1
    assert len(stored_files(store, "meti_fit_capacity")["PublicInfoSummary.html"]) == 2


def test_a_release_replaced_in_place_is_fetched_again(tmp_path):
    # Its link stays the same; only Last-Modified tells (notebook 04, section 6.6).
    store = LocalStore(tmp_path)
    assert fit_run(store, client=fit_client()).ok
    replaced = {"idB2603": "Mon, 30 Nov 2026 02:00:00 GMT"}

    seen = []
    again = fit_run(store, days_later=1, client=fit_client(seen=seen, last_modified=replaced))

    assert again.ok
    assert seen == ["PublicInfoSummary", "HEAD idA2603", "HEAD idB2603", "idB2603"]
    first_b, second_b = stored_files(store, "meti_fit_capacity")["B_city202603.xlsx"]
    assert store.get(second_b) == b"workbook idB2603 of Mon, 30 Nov 2026 02:00:00 GMT"
    assert json.loads(store.get(second_b + ".meta.json"))["load_type"] == "daily"
    assert len(stored_files(store, "meti_fit_capacity")["A_pref202603.xlsx"]) == 1
    assert again.unchanged == ["A_pref202603.xlsx"]


def test_the_same_last_modified_written_differently_is_not_a_replacement(tmp_path):
    store = LocalStore(tmp_path)
    assert fit_run(store, client=fit_client()).ok
    same_time = {"idA2603": "Tue, 18 Aug 2026 04:42:27 +0000", "idB2603": FIT_LAST_MODIFIED}

    again = fit_run(store, days_later=1, client=fit_client(last_modified=same_time))

    assert again.ok and sorted(again.unchanged) == ["A_pref202603.xlsx", "B_city202603.xlsx"]


def test_a_held_release_answering_without_last_modified_fails_the_capture(tmp_path):
    # It could no longer tell a replacement from none; downloading every day would hide that.
    store = LocalStore(tmp_path)
    assert fit_run(store, client=fit_client()).ok

    seen = []
    again = fit_run(
        store, days_later=1, client=fit_client(seen=seen, last_modified={"idA2603": None})
    )

    assert not again.ok and "A_pref202603.xlsx" in again.failures[0]
    assert "Last-Modified" in again.failures[0]
    assert "idA2603" not in seen  # nothing downloaded for it
    assert again.unchanged == ["B_city202603.xlsx"]


def test_initial_reads_the_index_again_even_when_a_daily_capture_stored_it(tmp_path):
    store = LocalStore(tmp_path)
    assert fit_run(store, client=fit_client()).ok

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/cdx"):
            return httpx.Response(200, content=b'[["timestamp"], ["20240302210103"]]')
        if request.url.path.endswith("PublicInfoSummary"):
            return httpx.Response(200, content=FIT_INDEX)
        return httpx.Response(200, content=b"archived page")

    history_client = httpx.Client(transport=httpx.MockTransport(handler))
    result = fit_run(store, mode="initial", days_later=1, client=history_client)

    assert result.ok
    assert sorted(result.skipped) == ["A_pref202603.xlsx", "B_city202603.xlsx"]
    names = {p.rsplit("/", 1)[-1].split("_", 1)[1] for p in result.stored}
    assert names == {
        "wayback_cdx.json",
        "PublicInfoSummary.html",
        "wayback_20240302210103_PublicInfoSummary.html",
    }
    (cdx_path,) = stored_files(store, "meti_fit_capacity")["wayback_cdx.json"]
    assert json.loads(store.get(cdx_path + ".meta.json"))["load_type"] == "initial"


def test_an_index_page_that_links_nothing_fails_the_capture(tmp_path):
    result = fit_run(LocalStore(tmp_path), client=fit_client(page=b"<html>maintenance</html>"))
    assert not result.ok
    assert "links no table A or B release" in result.failures[0]
