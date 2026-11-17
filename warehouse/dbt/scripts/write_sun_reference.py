"""Write seeds/dim_sun_reference.csv: the sun as NREL's SPA (through pvlib) puts it, sampled
exactly as dim_sun_slots samples it, for a data test to compare the SQL against within a tolerance.

Run from the repository root after changing the points or the sampling:

    uv run python warehouse/dbt/scripts/write_sun_reference.py
"""

from pathlib import Path

import pandas as pd
import pvlib

from jtf.ingest.sources import WEATHER_POINTS

# Solstices, equinoxes and days between, across the years the warehouse serves.
DAYS = [
    "2024-03-20", "2024-06-21", "2024-09-22", "2024-12-21",
    "2025-02-14", "2025-05-12", "2025-08-08", "2025-11-21",
    "2026-01-01", "2026-04-15", "2026-07-30", "2026-10-31",
]  # fmt: skip
SAMPLES_S = [150, 450, 750, 1050, 1350, 1650]  # the 5-minute middles of a slot, seconds in
OUT = Path(__file__).resolve().parents[1] / "seeds" / "dim_sun_reference.csv"


def slot_starts() -> pd.DatetimeIndex:
    days = [pd.date_range(f"{d} 00:00", periods=48, freq="30min", tz="Asia/Tokyo") for d in DAYS]
    return days[0].append(days[1:]).tz_convert("UTC")


def sun(times: pd.DatetimeIndex, lat: float, lon: float) -> pd.DataFrame:
    spa = pvlib.solarposition.get_solarposition(times, lat, lon, method="nrel_numpy")
    ghi = pvlib.clearsky.haurwitz(spa["zenith"])["ghi"]  # the geometric zenith, as the SQL
    return pd.DataFrame({"elevation": spa["elevation"], "ghi": ghi}, index=times)


def main() -> None:
    starts = slot_starts()
    rows = []
    for point, lat, lon in WEATHER_POINTS:
        edges = sun(starts.append(starts + pd.Timedelta(seconds=1800)).unique(), lat, lon)
        samples = [
            sun(starts + pd.Timedelta(seconds=s), lat, lon)["ghi"].to_numpy() for s in SAMPLES_S
        ]
        clear_sky = sum(samples) / len(samples)
        for i, start in enumerate(starts):
            end = start + pd.Timedelta(seconds=1800)
            rows.append(
                {
                    "point": point,
                    "slot_start": start.strftime("%Y-%m-%d %H:%M:%S+00:00"),
                    "elevation_start": round(edges.loc[start, "elevation"], 6),
                    "elevation_end": round(edges.loc[end, "elevation"], 6),
                    "clear_sky_ghi": round(clear_sky[i], 6),
                }
            )
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"{len(rows)} rows written to {OUT}")


if __name__ == "__main__":
    main()
