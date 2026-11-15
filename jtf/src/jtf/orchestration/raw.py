"""Raw assets: one per capture source, written by the capture jobs outside Dagster.

Dagster doesn't produce these files, it observes them: the sensor records each day's new files
as a materialization of that source's partition, and assets downstream react to it.
"""

import json
from datetime import datetime, timedelta

import dagster as dg

from jtf.ingest.landed import landed_since
from jtf.ingest.sources import JST, SOURCES, Source
from jtf.orchestration.resources import RawBucket

# Extraction day in JST, the raw layout's own partition. The first capture landed on this day;
# end_offset=1 keeps today's partition open while today's files land.
EXTRACTION_DAYS = dg.DailyPartitionsDefinition(
    start_date="2026-11-15", timezone="Asia/Tokyo", end_offset=1
)


def _cron(deadline: str, earliest: timedelta) -> dg.FreshnessPolicy:
    return dg.FreshnessPolicy.cron(
        deadline_cron=deadline, lower_bound_delta=earliest, timezone="Asia/Tokyo"
    )


# When a scheduled capture must have landed, given the capture schedules in deploy/terraform:
# 08:45 for most sources, 11:00 for JEPX, 19:00 and 22:00 for the next-day files.
# The window opens a little before the capture so a late sensor tick still counts.
MORNING = _cron("0 9 * * *", timedelta(minutes=30))
FRESHNESS = {
    "jepx_spot": _cron("30 11 * * *", timedelta(minutes=45)),
    "tepco_area_forecast_next_day": _cron("30 19,22 * * *", timedelta(minutes=45)),
    "tepco_area_bg_plan_next_day": _cron("30 19,22 * * *", timedelta(minutes=45)),
}


def raw_key(source: str) -> dg.AssetKey:
    return dg.AssetKey(["raw", source])


def raw_spec(source: Source) -> dg.AssetSpec:
    scheduled = source.items is not None
    return dg.AssetSpec(
        raw_key(source.name),
        description=f"Files captured by the jtf-fetch-{source.slug} job, unchanged, with sidecars",
        group_name="raw",
        kinds={"gcs"},
        partitions_def=EXTRACTION_DAYS,
        freshness_policy=FRESHNESS.get(source.name, MORNING) if scheduled else None,
        metadata={"modes": ", ".join(source.modes)},
    )


RAW_ASSETS = [raw_spec(source) for source in SOURCES.values()]


@dg.sensor(minimum_interval_seconds=300, default_status=dg.DefaultSensorStatus.RUNNING)
def raw_bucket_sensor(
    context: dg.SensorEvaluationContext, raw_bucket: RawBucket
) -> dg.SensorResult:
    """Lists the raw bucket and records each source's newly landed files, per extraction day.

    The cursor holds the newest path seen per source.
    """
    watermarks = json.loads(context.cursor) if context.cursor else {}
    store = raw_bucket.store()
    today = datetime.now(JST).date()
    events = []
    for source in SOURCES:
        for landed in landed_since(store, source, watermarks.get(source), today):
            events.append(
                dg.AssetMaterialization(
                    asset_key=raw_key(source),
                    partition=landed.extraction_date,
                    metadata={
                        "files": len(landed.paths),
                        "newest": landed.paths[-1],
                        "paths": dg.MetadataValue.json(list(landed.paths[-50:])),
                    },
                )
            )
            watermarks[source] = landed.paths[-1]
    context.log.info(f"{len(events)} source-days with new files")
    return dg.SensorResult(asset_events=events, cursor=json.dumps(watermarks, sort_keys=True))
