"""Open-Meteo weather: Previous Runs, Single Runs and the live forecast, one JSON shape.

A response is a list with one entry per point, each with `hourly` arrays whose names pack the
variable, the lead and the model: `shortwave_radiation_previous_day2_jma_msm`; with a single
model the model suffix is dropped. The tables are semi-long: one row per point, model, lead (or
run) and hour, one column per variable. Hours are UTC as the API gives them; radiation at hour t
is the mean of the hour before. A row where every variable is null (beyond a run's horizon, a
hole in the archive) is dropped. Recorded gaps never reach the parser.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pandas as pd

from jtf.ingest.sources import WEATHER_POINTS
from jtf.parse.base import Column, Parser, RawFile, Refused

VERSION = "1"
POINTS = [name for name, _, _ in WEATHER_POINTS]
# Open-Meteo's defaults, asserted on every file: a changed default fails the file.
UNITS = {
    "shortwave_radiation": "W/m²",
    "direct_radiation": "W/m²",
    "diffuse_radiation": "W/m²",
    "cloud_cover": "%",
    "temperature_2m": "°C",
    "wind_speed_10m": "km/h",
}
VARIABLES = list(UNITS)


def split_column(column: str, models: list[str]) -> tuple[str, int, str]:
    """'shortwave_radiation_previous_day2_jma_msm' -> ('shortwave_radiation', 2, 'jma_msm')."""
    model = next((m for m in models if column.endswith("_" + m)), None)
    if model is None:
        if len(models) != 1:
            raise Refused(f"column {column!r} names none of the requested models")
        model, base = models[0], column
    else:
        base = column.removesuffix("_" + model)
    variable, _, lead = base.partition("_previous_day")
    return variable, int(lead or 0), model


def parse_points(raw: RawFile) -> pd.DataFrame:
    data = json.loads(raw.body)
    data = data if isinstance(data, list) else [data]
    if len(data) != len(POINTS):
        raise Refused(f"{len(data)} points, expected {len(POINTS)}")
    models = raw.meta["params"]["models"].split(",")
    frames = []
    for point, entry in zip(POINTS, data, strict=True):
        if (entry.get("timezone"), entry.get("utc_offset_seconds")) != ("GMT", 0):
            raise Refused(f"time zone {entry.get('timezone')!r}, expected GMT")
        groups: dict[tuple[str, int], dict[str, list]] = defaultdict(dict)
        for column, values in entry["hourly"].items():
            if column == "time":
                continue
            variable, lead, model = split_column(column, models)
            unit = entry["hourly_units"][column]
            if UNITS.get(variable) != unit:
                raise Refused(f"{column} is in {unit!r}, expected {UNITS.get(variable)!r}")
            groups[(model, lead)][variable] = values
        for (model, lead), columns in groups.items():
            frame = pd.DataFrame(columns).reindex(columns=VARIABLES).astype("Float64")
            frame.insert(
                0, "valid_time", pd.to_datetime(entry["hourly"]["time"]).tz_localize("UTC")
            )
            frame.insert(0, "lead_days", lead)
            frame.insert(0, "model", model)
            frame.insert(0, "point", point)
            frame["grid_latitude"] = float(entry["latitude"])
            frame["grid_longitude"] = float(entry["longitude"])
            frames.append(frame)
    df = pd.concat(frames, ignore_index=True).dropna(how="all", subset=VARIABLES)
    return df.assign(**raw.lineage(VERSION)).reset_index(drop=True)


def _without_leads(df: pd.DataFrame) -> pd.DataFrame:
    if df["lead_days"].ne(0).any():
        raise Refused("lead variables in a response that should have none")
    return df.drop(columns="lead_days")


def parse_previous_runs(raw: RawFile) -> pd.DataFrame:
    return parse_points(raw)


def parse_single_run(raw: RawFile) -> pd.DataFrame:
    run_time = pd.Timestamp(raw.meta["params"]["run"], tz="UTC")
    return _without_leads(parse_points(raw)).assign(run_time=run_time)


def parse_live(raw: RawFile) -> pd.DataFrame:
    return _without_leads(parse_points(raw))


def _check(key: list[str]):
    def check(df: pd.DataFrame) -> list[str]:
        duplicates = int(df.duplicated(key).sum()) if not df.empty else 0
        return [f"{duplicates} duplicate rows for {', '.join(key)}"] if duplicates else []

    return check


def _columns(*key: Column) -> tuple[Column, ...]:
    return (
        Column("point", "STRING", required=True),
        Column("model", "STRING", required=True),
        *key,
        Column("valid_time", "TIMESTAMP", required=True),
        *(Column(v, "FLOAT") for v in VARIABLES),
        Column("grid_latitude", "FLOAT", required=True),
        Column("grid_longitude", "FLOAT", required=True),
    )


PREVIOUS_RUNS = Parser(
    source="openmeteo_previous_runs",
    version=VERSION,
    columns=_columns(Column("lead_days", "INTEGER", required=True)),
    parse=parse_previous_runs,
    check=_check(["point", "model", "lead_days", "valid_time"]),
)
SINGLE_RUNS = Parser(
    source="openmeteo_single_runs",
    version=VERSION,
    columns=_columns(Column("run_time", "TIMESTAMP", required=True)),
    parse=parse_single_run,
    check=_check(["point", "model", "run_time", "valid_time"]),
)
LIVE = Parser(
    source="openmeteo_live",
    version=VERSION,
    columns=_columns(),
    parse=parse_live,
    check=_check(["point", "model", "valid_time"]),
)
PARSERS = [PREVIOUS_RUNS, SINGLE_RUNS, LIVE]
