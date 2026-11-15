"""What each capture group fetches. URLs and request shapes were checked on the live sources."""

from __future__ import annotations

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

GROUPS = ("morning", "jepx", "evening")


@dataclass(frozen=True)
class FetchItem:
    source: str
    filename: str
    url: str
    method: str = "GET"
    params: dict[str, str] = field(default_factory=dict)
    form: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)


def _months_to_capture(now_jst: datetime) -> list[str]:
    """The current month, plus the previous one early in a month, when late corrections land."""
    months = [now_jst.strftime("%Y%m")]
    if now_jst.day <= 5:
        months.insert(0, (now_jst.replace(day=1) - timedelta(days=1)).strftime("%Y%m"))
    return months


def fiscal_year(day: datetime) -> int:
    """JEPX files are per Japanese fiscal year, which starts in April."""
    return day.year if day.month >= 4 else day.year - 1


def open_meteo_live() -> FetchItem:
    return FetchItem(
        source="openmeteo_live",
        filename="forecast_jma.json",
        url=OPEN_METEO_FORECAST,
        params={
            "latitude": ",".join(f"{lat}" for _, lat, _ in WEATHER_POINTS),
            "longitude": ",".join(f"{lon}" for _, _, lon in WEATHER_POINTS),
            "hourly": ",".join(WEATHER_VARIABLES),
            "models": ",".join(WEATHER_MODELS),
            "forecast_days": "3",
        },
    )


def items_for_group(group: str, now_jst: datetime) -> list[FetchItem]:
    if group == "morning":
        items = [
            FetchItem(
                "tepco_actuals", f"eria_jukyu_{ym}_03.csv", f"{TEPCO_WWW}/eria_jukyu_{ym}_03.csv"
            )
            for ym in _months_to_capture(now_jst)
        ]
        items += [
            FetchItem("tepco_sameday", name, f"{TEPCO_WWW4}/{name}")
            for name in ("AREA_YOSOKU.csv", "AREA_BGKEI.csv", "AREA_JISEKI.csv")
        ]
        items.append(FetchItem("tepco_sameday", "juyo-d1-j.csv", f"{TEPCO_WWW}/juyo-d1-j.csv"))
        items.append(open_meteo_live())
        return items
    if group == "jepx":
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
    if group == "evening":
        return [
            FetchItem("tepco_forecast_next_day", name, f"{TEPCO_WWW4}/{name}")
            for name in ("AREA_ONCE_YOSOKU.csv", "AREA_ONCE_BGKEI.csv")
        ]
    raise ValueError(f"Unknown capture group {group!r}; expected one of {GROUPS}")
