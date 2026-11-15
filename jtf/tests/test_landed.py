from datetime import date

from jtf.ingest.landed import landed_since
from jtf.storage import LocalStore

TODAY = date(2026, 11, 16)


def land(store, path, sidecar=True):
    store.put_new(path, b"data", "text/csv")
    if sidecar:
        store.put_new(path + ".meta.json", b"{}", "application/json")


def test_first_listing_groups_everything_by_extraction_day(tmp_path):
    store = LocalStore(tmp_path)
    land(store, "tepco_actuals/2026-11-15/20261115T020416Z_eria_jukyu_202611_03.csv")
    land(store, "tepco_actuals/2026-11-15/20261115T045240Z_area-2016.csv")
    land(store, "tepco_actuals/2026-11-16/20261115T234501Z_eria_jukyu_202611_03.csv")
    land(store, "jepx_spot/2026-11-16/20261116T020001Z_spot_summary_2026.csv")

    landed = landed_since(store, "tepco_actuals", None, TODAY)

    assert [(day.extraction_date, len(day.paths)) for day in landed] == [
        ("2026-11-15", 2),
        ("2026-11-16", 1),
    ]
    assert landed[0].paths[0].endswith("eria_jukyu_202611_03.csv")


def test_only_files_after_the_watermark_count(tmp_path):
    store = LocalStore(tmp_path)
    seen = "tepco_actuals/2026-11-15/20261115T020416Z_eria_jukyu_202611_03.csv"
    land(store, seen)
    land(store, "tepco_actuals/2026-11-16/20261115T234501Z_eria_jukyu_202611_03.csv")

    landed = landed_since(store, "tepco_actuals", seen, TODAY)

    assert [day.paths for day in landed] == [
        ("tepco_actuals/2026-11-16/20261115T234501Z_eria_jukyu_202611_03.csv",)
    ]
    assert landed_since(store, "tepco_actuals", landed[-1].paths[-1], TODAY) == []


def test_a_file_lands_only_once_its_sidecar_exists(tmp_path):
    store = LocalStore(tmp_path)
    path = "jepx_spot/2026-11-16/20261116T020001Z_spot_summary_2026.csv"
    land(store, path, sidecar=False)
    assert landed_since(store, "jepx_spot", None, TODAY) == []

    store.put_new(path + ".meta.json", b"{}", "application/json")
    assert landed_since(store, "jepx_spot", None, TODAY)[0].paths == (path,)


def test_days_after_today_are_not_listed(tmp_path):
    store = LocalStore(tmp_path)
    seen = "jepx_spot/2026-11-15/20261115T020001Z_spot_summary_2026.csv"
    land(store, seen)
    land(store, "jepx_spot/2026-11-17/20261117T020001Z_spot_summary_2026.csv")
    assert landed_since(store, "jepx_spot", seen, TODAY) == []
