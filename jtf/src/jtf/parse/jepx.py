"""JEPX day-ahead results, `spot_summary_{FY}.csv`: one row per delivery day and slot code 1–48.

One file per fiscal year; the current year's file grows each day and already holds tomorrow by
11:00. Volumes are whole kWh, prices are yen/kWh with decimals; the market's floor is 0.01.
JEPX sends no `Last-Modified`, so `source_time` is null.
"""

from __future__ import annotations

import io

import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused, decimals, whole_numbers

VERSION = "1"
COLUMNS = {
    "受渡日": "delivery_date",
    "時刻コード": "slot_code",
    "売り入札量(kWh)": "sell_bid_kwh",
    "買い入札量(kWh)": "buy_bid_kwh",
    "約定総量(kWh)": "contracted_kwh",
    "システムプライス(円/kWh)": "system_price_yen_kwh",
    "エリアプライス北海道(円/kWh)": "price_hokkaido_yen_kwh",
    "エリアプライス東北(円/kWh)": "price_tohoku_yen_kwh",
    "エリアプライス東京(円/kWh)": "price_tokyo_yen_kwh",
    "エリアプライス中部(円/kWh)": "price_chubu_yen_kwh",
    "エリアプライス北陸(円/kWh)": "price_hokuriku_yen_kwh",
    "エリアプライス関西(円/kWh)": "price_kansai_yen_kwh",
    "エリアプライス中国(円/kWh)": "price_chugoku_yen_kwh",
    "エリアプライス四国(円/kWh)": "price_shikoku_yen_kwh",
    "エリアプライス九州(円/kWh)": "price_kyushu_yen_kwh",
    "売りブロック入札総量(kWh)": "sell_block_bid_kwh",
    "売りブロック約定総量(kWh)": "sell_block_contracted_kwh",
    "買いブロック入札総量(kWh)": "buy_block_bid_kwh",
    "買いブロック約定総量(kWh)": "buy_block_contracted_kwh",
}
VOLUMES = [name for name in COLUMNS.values() if name.endswith("_kwh") and "price" not in name]
PRICES = [name for name in COLUMNS.values() if name.endswith("_yen_kwh")]
VOLUMES_AND_PRICES = list(COLUMNS.values())[2:]  # in the file's order


def parse(raw: RawFile) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(raw.text()), dtype=str)
    unknown = sorted(set(df.columns) - set(COLUMNS))
    if unknown:
        raise Refused(f"no ASCII name for columns {unknown}")
    df = df.rename(columns=COLUMNS).reindex(columns=list(COLUMNS.values()))
    df.insert(0, "row_number", range(1, len(df) + 1))
    df = decimals(whole_numbers(df, ["slot_code", *VOLUMES]), PRICES)
    return df.assign(**raw.lineage(VERSION))


def check(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    issues = []
    duplicates = int(df.duplicated(["delivery_date", "slot_code"]).sum())
    if duplicates:
        issues.append(f"{duplicates} duplicate slots")
    if df.groupby("delivery_date")["slot_code"].nunique().ne(48).any():
        issues.append("a delivery day without 48 slot codes")
    if df["price_tokyo_yen_kwh"].lt(0.01).any():
        issues.append("a Tokyo price below the market's floor of 0.01")
    return issues


PARSER = Parser(
    source="jepx_spot",
    version=VERSION,
    columns=(
        Column("row_number", "INTEGER", required=True),
        Column("delivery_date", "STRING", required=True),
        Column("slot_code", "INTEGER", required=True),
        *(Column(name, "FLOAT" if name in PRICES else "INTEGER") for name in VOLUMES_AND_PRICES),
    ),
    parse=parse,
    check=check,
)
