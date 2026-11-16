"""ODS assets: one per parser, partitioned like the raw files they read.

Materialising day D parses every file of D not yet loaded at the parser's version, so a re-run
adds nothing. The parser's version is the asset's code version: changing a parser marks every
partition out of date, and the backfill appends rows under the new version.
"""

from datetime import UTC, datetime

import dagster as dg

from jtf.orchestration.raw import EXTRACTION_DAYS, raw_key
from jtf.orchestration.resources import BigQuery, RawBucket
from jtf.parse import PARSERS
from jtf.parse.base import Parser
from jtf.parse.load import parse_day

FILE_CHECKS = "files_pass_checks"


def ods_key(parser: Parser) -> dg.AssetKey:
    return dg.AssetKey(["ods", parser.table])


def ods_asset(parser: Parser) -> dg.AssetsDefinition:
    key = ods_key(parser)

    @dg.asset(
        key=key,
        deps=[raw_key(parser.source)],
        partitions_def=EXTRACTION_DAYS,
        code_version=parser.version,
        automation_condition=dg.AutomationCondition.eager(),
        group_name="ods",
        kinds={"bigquery"},
        description=f"Rows of {parser.source}'s raw files, with lineage; one row per data row",
        check_specs=[
            dg.AssetCheckSpec(
                FILE_CHECKS,
                asset=key,
                description="Every parsed file passes its per-file checks: slots, keys, ranges",
            )
        ],
    )
    def _ods(
        context: dg.AssetExecutionContext, raw_bucket: RawBucket, bigquery: BigQuery
    ) -> dg.MaterializeResult:
        result = parse_day(
            parser,
            raw_bucket.store(),
            bigquery.warehouse(),
            context.partition_key,
            batch_id=context.run.run_id,
            now=datetime.now(UTC),
        )
        context.log.info(result.summary())
        if result.failed:
            # The files that parsed are loaded; the refused ones fail the partition by name.
            raise dg.Failure(
                description=f"{len(result.failed)} file(s) refused by the {parser.source} parser",
                metadata={path: reason for path, reason in result.failed.items()},
            )
        return dg.MaterializeResult(
            metadata={
                "files_loaded": len(result.loaded),
                "rows": result.rows,
                "files_skipped": result.skipped,
                "files_empty": len(result.empty),
                "files_gaps": len(result.gaps),
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=FILE_CHECKS,
                    passed=not result.flagged,
                    metadata={"flagged": dg.MetadataValue.json(result.flagged)},
                )
            ],
        )

    return _ods


# A parser is proven with `jtf parse` first; it becomes an asset once its table is listed here.
ORCHESTRATED = (
    "tepco_actuals",
    "tepco_actuals_yearly",
    "tepco_area_forecast_next_day",
    "tepco_area_bg_plan_next_day",
    "tepco_area_forecast",
    "tepco_area_bg_plan",
    "tepco_area_actuals_today",
    "tepco_demand_outlook_peaks",
    "tepco_demand_outlook_hourly",
    "tepco_demand_outlook_5min",
    "jepx_spot",
    "openmeteo_previous_runs",
    "openmeteo_single_runs",
    "openmeteo_live",
    "meti_fit_capacity_pref",
    "meti_fit_capacity_city",
    "meti_fit_notices",
)
ODS_ASSETS = [ods_asset(PARSERS[table]) for table in ORCHESTRATED]
