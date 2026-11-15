"""Raw files to ODS rows, one parser per file layout.

Parsing changes the format, never the meaning: one row per data row, ASCII column names, typed
values, the source's own units and time labels, and the lineage that DWD needs to work out
versions and `known_at`. A parser refuses a file whose shape changed rather than write wrong rows.
The design, and a run over every raw file, is in notebooks/02_raw_to_ods.ipynb.
"""

from jtf.parse import (
    jepx,
    openmeteo,
    tepco_actuals,
    tepco_actuals_yearly,
    tepco_area,
    tepco_demand_outlook,
)
from jtf.parse.base import Parser

# By ODS table: a source with several file layouts has one parser, and one table, per layout.
PARSERS: dict[str, Parser] = {
    parser.table: parser
    for parser in (
        tepco_actuals.PARSER,
        tepco_actuals_yearly.PARSER,
        *tepco_area.PARSERS,
        *tepco_demand_outlook.PARSERS,
        jepx.PARSER,
        *openmeteo.PARSERS,
    )
}


def parsers_for(source: str) -> list[Parser]:
    found = [parser for parser in PARSERS.values() if parser.source == source]
    if not found:
        known = sorted({parser.source for parser in PARSERS.values()})
        raise ValueError(f"No parser for {source!r} yet; parsed sources: {known}")
    return found
