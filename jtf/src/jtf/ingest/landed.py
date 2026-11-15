"""Which captured files have landed in the raw bucket since a watermark.

The Dagster raw-bucket sensor calls this on every tick. Within a source, paths sort in landing
order (`{source}/{extraction_date}/{fetched_at}_{filename}`, and fetch spaces items at least a
second apart), so the newest path seen so far is the watermark. A file has landed once its
sidecar exists: fetch writes the sidecar last.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from jtf.storage import Store

SIDECAR = ".meta.json"


@dataclass(frozen=True)
class Landed:
    source: str
    extraction_date: str
    paths: tuple[str, ...]
    """Data files, oldest first."""


def landed_since(store: Store, source: str, after: str | None, today: date) -> list[Landed]:
    """Files of `source` that landed after the path `after`, grouped by extraction day.

    Without a watermark the whole prefix is listed. With one, only the days from the
    watermark's day to `today` are, so a tick stays cheap however long the history grows.
    """
    if after is None:
        listed = store.list(f"{source}/")
    else:
        day = date.fromisoformat(after.split("/")[1])
        listed = []
        while day <= today:
            listed += store.list(f"{source}/{day.isoformat()}/")
            day += timedelta(days=1)
    present = set(listed)
    by_day: dict[str, list[str]] = defaultdict(list)
    for path in sorted(listed):
        if path.endswith(SIDECAR) or path + SIDECAR not in present:
            continue
        if after is not None and path <= after:
            continue
        by_day[path.split("/")[1]].append(path)
    return [Landed(source, day, tuple(paths)) for day, paths in sorted(by_day.items())]
