"""Capture sources: one source file per source, each with its own retry policy and pacing.

URLs and request shapes were checked against the live sources; the history ranges and costs come
from notebooks/01_history_probe.ipynb. A source name uses underscores (Dagster asset keys and
BigQuery tables need them); job names and health-check slugs swap them for hyphens.

A source can provide three lists of items:
- `items`: what the daily scheduled capture fetches
- `history`: what the one-off initial load fetches
- `recapture`: what the periodic re-capture checks for rewrites, with `If-Modified-Since`

Most items are built from name patterns and dates. One source can't be: the FIT/FIP disclosure
links its files by unguessable ids, so its index page is an item whose stored body expands into
the next items (`FetchItem.expand`). Capture reads that page for links only, never a data file.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta, timezone
from html import unescape

JST = timezone(timedelta(hours=9), name="JST")

TEPCO_WWW = "https://www.tepco.co.jp/forecast/html/images"
TEPCO_WWW4 = "https://www4.tepco.co.jp/forecast/html/images"
JEPX_DOWNLOAD = "https://www.jepx.jp/_download.php"
JEPX_SPOT_PAGE = "https://www.jepx.jp/electricpower/market-data/spot/"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_PREVIOUS_RUNS = "https://previous-runs-api.open-meteo.com/v1/forecast"
OPEN_METEO_SINGLE_RUNS = "https://single-runs-api.open-meteo.com/v1/forecast"
FIT_PAGE = "https://www.fit-portal.go.jp/PublicInfoSummary"
FIT_FILE = "https://www.fit-portal.go.jp/servlet/servlet.FileDownload"
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
WAYBACK = "https://web.archive.org/web"

# One point per prefecture in the TEPCO area. Their weights are each prefecture's share of solar
# capacity as of T, derived in the warehouse (notebooks/03_solar_capacity.ipynb, section 8).
WEATHER_POINTS: tuple[tuple[str, float, float], ...] = (
    ("tokyo", 35.69, 139.69),
    ("yokohama", 35.44, 139.64),
    ("saitama", 35.86, 139.65),
    ("chiba", 35.61, 140.12),
    ("mito", 36.37, 140.47),
    ("utsunomiya", 36.56, 139.88),
    ("maebashi", 36.39, 139.06),
    ("kofu", 35.66, 138.57),
    ("numazu", 35.10, 138.86),
)
# MSM is the main solar input. Through Open-Meteo, GSM has no radiation, so ECMWF IFS 0.25°
# is the radiation fallback; GSM stays for cloud cover and temperature.
WEATHER_MODELS = ("jma_msm", "jma_gsm", "ecmwf_ifs025")
WEATHER_VARIABLES = (
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "cloud_cover",
    "temperature_2m",
    "wind_speed_10m",
)
# Previous Runs leads: "latest" and previous_day1 exist only for the lookahead-leak scenario (F2);
# features use previous_day2.
WEATHER_LEADS = ("", "_previous_day1", "_previous_day2")
# Single runs per model and day: the newest run that could be available at 09:00 JST, and a safer
# older one. The as-of rule picks between them.
SINGLE_RUNS: tuple[tuple[str, int], ...] = (
    ("jma_msm", 18),
    ("jma_msm", 21),
    ("jma_gsm", 12),
    ("jma_gsm", 18),
    ("ecmwf_ifs025", 12),
    ("ecmwf_ifs025", 18),
)

TEPCO_MONTHLY_FROM = date(2024, 2, 1)
# TEPCO's archive of the demand outlook: one zip a month of end-of-day daily files.
POWER_USAGE_FROM = date(2022, 4, 1)
TEPCO_YEARLY_FISCAL_YEARS = range(2016, 2024)
JEPX_FROM_FISCAL_YEAR = 2016
PREVIOUS_RUNS_FROM = date(2024, 1, 1)
SINGLE_RUNS_FROM = date(2026, 4, 2)
# FIT/FIP releases published as .xls (the older Excel format), which the parser doesn't read; every
# other release since June 2017 is .xlsx (notebooks/03_solar_capacity.ipynb, section 4).
FIT_XLS_RELEASES = frozenset({"201709", "201712", "201803"})


@dataclass(frozen=True)
class FetchItem:
    source: str
    filename: str
    url: str
    method: str = "GET"
    params: dict[str, str] = field(default_factory=dict)
    form: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    expand: Callable[[bytes], list[FetchItem]] | None = field(default=None, compare=False)
    """An index page: once stored, its body gives the items to fetch next. Always fetched."""
    watched: bool = False
    """For a file the source may replace in place, under the same link. The daily capture fetches
    it if no file of this name is stored yet; after that it asks for its headers only (`HEAD`) and
    fetches it again only when `Last-Modified` differs from the newest stored copy's."""


@dataclass(frozen=True)
class RetryPolicy:
    timeout_s: float
    connect_timeout_s: float
    delays_s: tuple[float, ...]
    """Waits between attempts; there is one more attempt than delays."""

    @property
    def attempts(self) -> int:
        return len(self.delays_s) + 1


STANDARD = RetryPolicy(timeout_s=30, connect_timeout_s=30, delays_s=(2, 5))
# Connections from Cloud Run to Open-Meteo sometimes fail to open; a fresh attempt succeeds.
PATIENT = RetryPolicy(timeout_s=60, connect_timeout_s=10, delays_s=(2, 5, 10, 20))


@dataclass(frozen=True)
class Pacing:
    """How long to wait after each request when a run fetches many items."""

    min_interval_s: float
    calls_per_minute: float | None = None
    """Open-Meteo weighs requests by size (X-RateLimit-Cost); spread that weight over the minute."""

    def wait_after(self, cost: float) -> float:
        wait = self.min_interval_s
        if self.calls_per_minute and cost:
            wait = max(wait, cost * 60 / self.calls_per_minute * 1.1)
        return wait


POLITE = Pacing(min_interval_s=2)
OPEN_METEO_PACING = Pacing(min_interval_s=1, calls_per_minute=600)
# Below the free 10,000 a day, leaving room for the daily live capture.
OPEN_METEO_DAILY_BUDGET = 8000.0

Items = Callable[[datetime], list[FetchItem]]
GapCheck = Callable[[int, bytes], bool]


def missing_run(status: int, body: bytes) -> bool:
    """Open-Meteo's answer for a run its archive doesn't have: a permanent gap, not an error."""
    return status == 400 and b"model run is not available" in body


@dataclass(frozen=True)
class Source:
    name: str
    items: Items | None = None
    history: Items | None = None
    recapture: Items | None = None
    retry: RetryPolicy = STANDARD
    pacing: Pacing = POLITE
    call_budget: float | None = None
    """Stop a run once this many weighted calls are spent; a later run resumes."""
    is_gap: GapCheck | None = None
    """Answers that mean "this item doesn't exist": stored as a recorded gap, not a failure."""

    @property
    def slug(self) -> str:
        return self.name.replace("_", "-")

    @property
    def modes(self) -> list[str]:
        return [m for m in ("daily", "initial", "recapture") if self.items_for(m) is not None]

    def items_for(self, mode: str) -> Items | None:
        return {"daily": self.items, "initial": self.history, "recapture": self.recapture}[mode]


def fiscal_year(day: datetime | date) -> int:
    """JEPX files are per Japanese fiscal year, which starts in April."""
    return day.year if day.month >= 4 else day.year - 1


def _month_starts(first: date, last: date) -> list[date]:
    months, current = [], first.replace(day=1)
    while current <= last:
        months.append(current)
        current = (current + timedelta(days=32)).replace(day=1)
    return months


def _yesterday_jst(now: datetime) -> date:
    return now.astimezone(JST).date() - timedelta(days=1)


# ---------------------------------------------------------------- TEPCO actuals


def _eria_jukyu(month: date) -> FetchItem:
    name = f"eria_jukyu_{month:%Y%m}_03.csv"
    return FetchItem("tepco_actuals", name, f"{TEPCO_WWW}/{name}")


def _tepco_actuals_daily(now: datetime) -> list[FetchItem]:
    """The current and the previous month: TEPCO keeps rewriting a month for weeks after it ends."""
    this_month = now.astimezone(JST).date().replace(day=1)
    previous = (this_month - timedelta(days=1)).replace(day=1)
    return [_eria_jukyu(previous), _eria_jukyu(this_month)]


def _tepco_actuals_monthly(now: datetime) -> list[FetchItem]:
    return [_eria_jukyu(m) for m in _month_starts(TEPCO_MONTHLY_FROM, now.astimezone(JST).date())]


def _tepco_actuals_history(now: datetime) -> list[FetchItem]:
    yearly = [
        FetchItem("tepco_actuals", f"area-{fy}.csv", f"{TEPCO_WWW}/area-{fy}.csv")
        for fy in TEPCO_YEARLY_FISCAL_YEARS
    ]
    return yearly + _tepco_actuals_monthly(now)


# ---------------------------------------------------------------- TEPCO demand outlook


def _power_usage(month: date) -> FetchItem:
    name = f"{month:%Y%m}_power_usage.zip"
    return FetchItem("tepco_demand_outlook", name, f"{TEPCO_WWW}/{name}")


def _demand_outlook_daily(now: datetime) -> list[FetchItem]:
    """Today's file, plus the archive months of yesterday and the month before.

    Yesterday's final version is added to its month's zip each morning, and recent months are
    rewritten without notice. Yesterday's month, not today's: on the 1st, this month's zip
    doesn't exist yet.
    """
    yesterday = _yesterday_jst(now).replace(day=1)
    before = (yesterday - timedelta(days=1)).replace(day=1)
    live = FetchItem("tepco_demand_outlook", "juyo-d1-j.csv", f"{TEPCO_WWW}/juyo-d1-j.csv")
    return [live, _power_usage(before), _power_usage(yesterday)]


def _power_usage_months(now: datetime) -> list[FetchItem]:
    return [_power_usage(m) for m in _month_starts(POWER_USAGE_FROM, _yesterday_jst(now))]


# ---------------------------------------------------------------- JEPX


def _jepx_file(fy: int) -> FetchItem:
    name = f"spot_summary_{fy}.csv"
    return FetchItem(
        "jepx_spot",
        name,
        JEPX_DOWNLOAD,
        method="POST",
        form={"dir": "spot_summary", "file": name},
        headers={"Referer": JEPX_SPOT_PAGE},
    )


def _jepx_daily(now: datetime) -> list[FetchItem]:
    return [_jepx_file(fiscal_year(now.astimezone(JST)))]


def _jepx_history(now: datetime) -> list[FetchItem]:
    """Past fiscal years; the current one comes from the daily capture."""
    return [_jepx_file(fy) for fy in range(JEPX_FROM_FISCAL_YEAR, fiscal_year(now.astimezone(JST)))]


# ---------------------------------------------------------------- Open-Meteo


def _points() -> dict[str, str]:
    return {
        "latitude": ",".join(f"{lat}" for _, lat, _ in WEATHER_POINTS),
        "longitude": ",".join(f"{lon}" for _, _, lon in WEATHER_POINTS),
    }


def _openmeteo_live(_now: datetime) -> list[FetchItem]:
    return [
        FetchItem(
            source="openmeteo_live",
            filename="forecast.json",
            url=OPEN_METEO_FORECAST,
            params={
                **_points(),
                "hourly": ",".join(WEATHER_VARIABLES),
                "models": ",".join(WEATHER_MODELS),
                "forecast_days": "3",
            },
        )
    ]


def _previous_runs_history(now: datetime) -> list[FetchItem]:
    """One request per calendar month up to yesterday; the file name carries the date range."""
    last = _yesterday_jst(now)
    items = []
    for start in _month_starts(PREVIOUS_RUNS_FROM, last):
        end = min((start + timedelta(days=32)).replace(day=1) - timedelta(days=1), last)
        items.append(
            FetchItem(
                source="openmeteo_previous_runs",
                filename=f"previous_runs_{start}_{end}.json",
                url=OPEN_METEO_PREVIOUS_RUNS,
                params={
                    **_points(),
                    "hourly": ",".join(
                        v + lead for v in WEATHER_VARIABLES for lead in WEATHER_LEADS
                    ),
                    "models": ",".join(WEATHER_MODELS),
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                },
            )
        )
    return items


def _single_runs_history(now: datetime) -> list[FetchItem]:
    """Every chosen run from the start of the archive until the day before yesterday (UTC)."""
    last = now.astimezone(UTC).date() - timedelta(days=2)
    items = []
    day = SINGLE_RUNS_FROM
    while day <= last:
        for model, hour in SINGLE_RUNS:
            run = f"{day}T{hour:02d}:00"
            items.append(
                FetchItem(
                    source="openmeteo_single_runs",
                    filename=f"{model}_{day}T{hour:02d}Z.json",
                    url=OPEN_METEO_SINGLE_RUNS,
                    params={
                        **_points(),
                        "hourly": ",".join(WEATHER_VARIABLES),
                        "models": model,
                        "run": run,
                    },
                )
            )
        day += timedelta(days=1)
    return items


# ---------------------------------------------------------------- FIT/FIP solar capacity

_FIT_LINK = re.compile(r'<a[^>]+href="[^"]*[?&]file=(\w+)"[^>]*>(.*?)</a>', re.S)
_FIT_LABEL = re.compile(r"([AB])表.*\((\d{4})年(\d{1,2})月末時点\)")
_FIT_NAMES = {"A": "A_pref{}.xlsx", "B": "B_city{}.xlsx"}


def fit_links(page: bytes) -> list[FetchItem]:
    """Every table A and B release the summary page links, named as the server names them.

    Raises if the page links none: its layout changed, and the capture should fail loudly.
    """
    items = []
    for file_id, inner in _FIT_LINK.findall(page.decode("utf-8", "replace")):
        label = unicodedata.normalize("NFKC", unescape(re.sub(r"<[^>]+>", "", inner)))
        if match := _FIT_LABEL.search(label):
            month = f"{match[2]}{int(match[3]):02d}"
            if month not in FIT_XLS_RELEASES:
                name = _FIT_NAMES[match[1]].format(month)
                item = FetchItem("meti_fit_capacity", name, FIT_FILE, params={"file": file_id})
                items.append(item)
    if not items:
        raise ValueError("the FIT/FIP summary page links no table A or B release")
    return items


def _fit_page(expand: Callable[[bytes], list[FetchItem]]) -> FetchItem:
    return FetchItem("meti_fit_capacity", "PublicInfoSummary.html", FIT_PAGE, expand=expand)


def _fit_daily(_now: datetime) -> list[FetchItem]:
    """The page every day (its notices date each release), any release not stored yet, and again
    any release whose `Last-Modified` changed: a replaced file keeps its link, and the server
    ignores `If-Modified-Since` (notebooks/04_ods_to_dwd.ipynb, section 6.6)."""
    return [_fit_page(lambda page: [replace(i, watched=True) for i in fit_links(page)])]


def _wayback_snapshots(cdx: bytes) -> list[FetchItem]:
    """Archived copies of the summary page: the only record of when old releases were announced."""
    stamps = [row[0] for row in json.loads(cdx)[1:]]
    return [
        FetchItem(
            "meti_fit_capacity",
            f"wayback_{stamp}_PublicInfoSummary.html",
            f"{WAYBACK}/{stamp}id_/{FIT_PAGE}",
        )
        for stamp in stamps
    ]


def _fit_history(_now: datetime) -> list[FetchItem]:
    cdx = FetchItem(
        "meti_fit_capacity",
        "wayback_cdx.json",
        WAYBACK_CDX,
        params={
            "url": "fit-portal.go.jp/PublicInfoSummary",
            "output": "json",
            "fl": "timestamp",
            "filter": "statuscode:200",
            "collapse": "digest",
        },
        expand=_wayback_snapshots,
    )
    return [cdx, _fit_page(fit_links)]


# ---------------------------------------------------------------- registry


def _single_file(name: str, base_url: str, filename: str) -> Source:
    return Source(name, items=lambda _now: [FetchItem(name, filename, f"{base_url}/{filename}")])


SOURCES: dict[str, Source] = {
    s.name: s
    for s in (
        Source(
            "tepco_actuals",
            items=_tepco_actuals_daily,
            history=_tepco_actuals_history,
            recapture=_tepco_actuals_monthly,
        ),
        _single_file("tepco_area_forecast", TEPCO_WWW4, "AREA_YOSOKU.csv"),
        _single_file("tepco_area_bg_plan", TEPCO_WWW4, "AREA_BGKEI.csv"),
        _single_file("tepco_area_actuals_today", TEPCO_WWW4, "AREA_JISEKI.csv"),
        Source(
            "tepco_demand_outlook",
            items=_demand_outlook_daily,
            history=_power_usage_months,
            recapture=_power_usage_months,
        ),
        _single_file("tepco_area_forecast_next_day", TEPCO_WWW4, "AREA_ONCE_YOSOKU.csv"),
        _single_file("tepco_area_bg_plan_next_day", TEPCO_WWW4, "AREA_ONCE_BGKEI.csv"),
        Source("openmeteo_live", items=_openmeteo_live, retry=PATIENT, pacing=OPEN_METEO_PACING),
        Source(
            "openmeteo_previous_runs",
            history=_previous_runs_history,
            retry=PATIENT,
            pacing=OPEN_METEO_PACING,
            call_budget=OPEN_METEO_DAILY_BUDGET,
        ),
        Source(
            "openmeteo_single_runs",
            history=_single_runs_history,
            retry=PATIENT,
            pacing=OPEN_METEO_PACING,
            call_budget=OPEN_METEO_DAILY_BUDGET,
            # JMA's archive starts around mid-May 2026, not 2 April, and has later holes.
            is_gap=missing_run,
        ),
        Source("jepx_spot", items=_jepx_daily, history=_jepx_history),
        Source("meti_fit_capacity", items=_fit_daily, history=_fit_history),
    )
}


def get_source(name: str) -> Source:
    try:
        return SOURCES[name]
    except KeyError:
        raise ValueError(f"Unknown source {name!r}; expected one of {sorted(SOURCES)}") from None
