"""The e2e tests' world: real raw files for target days 15 to 19 November 2026, decided at 09:00
JST on the 14th to the 18th, the seam at capture start (e2e_tests/README.md, The test world).

Copied from the raw bucket into `raw/`, in the bucket's layout, with each file's sidecar. The city
spreadsheets of the capacity releases are cut to what the warehouse reads: each sheet's title and
header rows, Shizuoka's cities (the TEPCO area's part east of the Fuji River) and the national
total the parser checks for. Their sidecars then describe the cut file and name the original.
Raw files never change and nothing extracted after the world's last day is taken, so running this
again gives the same world. Run by hand, with the bucket in `JTF_RAW_URL`:

    set -a; . ./.env; set +a; uv run python e2e_tests/world/make_world.py
"""

import hashlib
import io
import json
import os
import re
import shutil
import zipfile
from pathlib import Path

import openpyxl

from jtf.ingest.landed import SIDECAR
from jtf.storage import open_store

WORLD = Path(__file__).parent / "raw"
LAST_DAY = "2026-11-18"
# The files the window's decision times read, by source (e2e_tests/README.md, The test world)
TAKE = {
    "tepco_actuals": re.compile(r"_eria_jukyu_202611_03\.csv$"),
    "openmeteo_previous_runs": re.compile(r"_previous_runs_2026-11-\d\d_2026-11-\d\d\.json$"),
    "openmeteo_single_runs": re.compile(r"_(?P<run>\d{4}-\d\d-\d\dT\d\dZ)\.json$"),
    "openmeteo_live": re.compile(r"_forecast(_jma)?\.json$"),
    "meti_fit_capacity": re.compile(
        r"_(A_pref|B_city)(202506|202509|202512|202603)\.xlsx$"
        r"|_wayback_20261027091515_PublicInfoSummary\.html$"
        r"|^\d{8}T\d{6}Z_PublicInfoSummary\.html$"
    ),
}
# The Single Runs that can be served at 09:00 on the 15th: the newest known before it per model
RUNS = ("2026-11-13T00Z", "2026-11-15T00Z")
CITY = re.compile(r"_B_city\d{6}\.xlsx$")
HEADER_ROWS = 5  # the title, a blank row, and three header rows (jtf/src/jtf/parse/meti_fit.py)


def taken(path: str) -> bool:
    source, day, name = path.split("/")
    match = TAKE[source].search(name)
    if not match or day > LAST_DAY or name.endswith(SIDECAR):
        return False
    if source == "openmeteo_single_runs":
        return RUNS[0] <= match["run"] <= RUNS[1]
    return True


def cut_city(body: bytes) -> bytes:
    """A city spreadsheet with only its header rows, Shizuoka's rows and the national total."""
    source = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    cut = openpyxl.Workbook()
    cut.remove(cut.active)
    for ws in source.worksheets:
        out = cut.create_sheet(ws.title)
        for number, row in enumerate(ws.iter_rows(values_only=True), start=1):
            area = row[0] if row else None
            keep = isinstance(area, str) and (area.startswith("静岡県") or area == "合計")
            if number <= HEADER_ROWS or keep:
                out.append(list(row))
    buffer = io.BytesIO()
    cut.save(buffer)
    return stable(buffer.getvalue())


def stable(xlsx: bytes) -> bytes:
    """The same workbook as the same bytes, run after run: openpyxl stamps the time of saving into
    the zip entries and the document's properties, so both are fixed."""
    fixed = "2026-11-18T00:00:00Z"
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(xlsx)) as source, zipfile.ZipFile(out, "w") as target:
        for entry in source.infolist():
            data = source.read(entry.filename)
            if entry.filename == "docProps/core.xml":
                stamp = r"(<dcterms:(?:created|modified)[^>]*>)[^<]*"
                data = re.sub(stamp, rf"\g<1>{fixed}", data.decode()).encode()
            info = zipfile.ZipInfo(entry.filename, date_time=(2026, 11, 18, 0, 0, 0))
            info.compress_type, info.external_attr = zipfile.ZIP_DEFLATED, entry.external_attr
            target.writestr(info, data)
    return out.getvalue()


def main() -> None:
    store = open_store(os.environ["JTF_RAW_URL"])
    shutil.rmtree(WORLD, ignore_errors=True)
    for source in TAKE:
        for path in sorted(p for p in store.list(f"{source}/") if taken(p)):
            body, meta = store.get(path), json.loads(store.get(path + SIDECAR))
            if CITY.search(path):
                original = meta["sha256"]
                body = cut_city(body)
                meta |= {
                    "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "cut": "for the e2e world: header rows, 静岡県 and 合計 only; "
                    f"the original's sha256 {original}",
                }
            target = WORLD / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
            (WORLD / (path + SIDECAR)).write_text(json.dumps(meta, ensure_ascii=False, indent=1))
    files = [f for f in WORLD.rglob("*") if f.is_file() and not f.name.endswith(SIDECAR)]
    print(f"{len(files)} files, {sum(f.stat().st_size for f in files) / 1e6:.1f} MB in {WORLD}")


if __name__ == "__main__":
    main()
