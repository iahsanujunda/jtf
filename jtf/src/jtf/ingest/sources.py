"""Capture sources: one source file per source, each with its own retry policy.

URLs and request shapes were checked against the live sources. A source name uses underscores
(Dagster asset keys and BigQuery tables need them); job names and health-check slugs swap them
for hyphens.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9), name="JST")

TEPCO_WWW = "https://www.tepco.co.jp/forecast/html/images"
TEPCO_WWW4 = "https://www4.tepco.co.jp/forecast/html/images"
JEPX_DOWNLOAD = "https://www.jepx.jp/_download.php"
JEPX_SPOT_PAGE = "https://www.jepx.jp/electricpower/market-data/spot/"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

# One point per prefecture in the TEPCO area, equal weights for now (phase 1 decision).
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


@dataclass(frozen=True)
class FetchItem:
    source: str
    filename: str
    url: str
    method: str = "GET"
    params: dict[str, str] = field(default_factory=dict)
    form: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)


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
PATIENT = RetryPolicy(timeout_s=30, connect_timeout_s=10, delays_s=(2, 5, 10, 20))


@dataclass(frozen=True)
class Source:
    name: str
    items: Callable[[datetime], list[FetchItem]]
    retry: RetryPolicy = STANDARD

    @property
    def slug(self) -> str:
        return self.name.replace("_", "-")


def fiscal_year(day: datetime) -> int:
    """JEPX files are per Japanese fiscal year, which starts in April."""
    return day.year if day.month >= 4 else day.year - 1


def _tepco_actuals(now_jst: datetime) -> list[FetchItem]:
    """The current month, plus the previous one early in a month, when late corrections land."""
    months = [now_jst.strftime("%Y%m")]
    if now_jst.day <= 5:
        months.insert(0, (now_jst.replace(day=1) - timedelta(days=1)).strftime("%Y%m"))
    return [
        FetchItem("tepco_actuals", f"eria_jukyu_{ym}_03.csv", f"{TEPCO_WWW}/eria_jukyu_{ym}_03.csv")
        for ym in months
    ]


def _openmeteo_live(_now: datetime) -> list[FetchItem]:
    return [
        FetchItem(
            source="openmeteo_live",
            filename="forecast.json",
            url=OPEN_METEO_FORECAST,
            params={
                "latitude": ",".join(f"{lat}" for _, lat, _ in WEATHER_POINTS),
                "longitude": ",".join(f"{lon}" for _, _, lon in WEATHER_POINTS),
                "hourly": ",".join(WEATHER_VARIABLES),
                "models": ",".join(WEATHER_MODELS),
                "forecast_days": "3",
            },
        )
    ]


def _jepx_spot(now_jst: datetime) -> list[FetchItem]:
    name = f"spot_summary_{fiscal_year(now_jst)}.csv"
    return [
        FetchItem(
            "jepx_spot",
            name,
            JEPX_DOWNLOAD,
            method="POST",
            form={"dir": "spot_summary", "file": name},
            headers={"Referer": JEPX_SPOT_PAGE},
        )
    ]


def _single_file(name: str, base_url: str, filename: str) -> Source:
    return Source(name, lambda _now: [FetchItem(name, filename, f"{base_url}/{filename}")])


SOURCES: dict[str, Source] = {
    s.name: s
    for s in (
        Source("tepco_actuals", _tepco_actuals),
        _single_file("tepco_area_forecast", TEPCO_WWW4, "AREA_YOSOKU.csv"),
        _single_file("tepco_area_bg_plan", TEPCO_WWW4, "AREA_BGKEI.csv"),
        _single_file("tepco_area_actuals_today", TEPCO_WWW4, "AREA_JISEKI.csv"),
        _single_file("tepco_demand_outlook", TEPCO_WWW, "juyo-d1-j.csv"),
        _single_file("tepco_area_forecast_next_day", TEPCO_WWW4, "AREA_ONCE_YOSOKU.csv"),
        _single_file("tepco_area_bg_plan_next_day", TEPCO_WWW4, "AREA_ONCE_BGKEI.csv"),
        Source("openmeteo_live", _openmeteo_live, retry=PATIENT),
        Source("jepx_spot", _jepx_spot),
    )
}


def get_source(name: str) -> Source:
    try:
        return SOURCES[name]
    except KeyError:
        raise ValueError(f"Unknown source {name!r}; expected one of {sorted(SOURCES)}") from None
