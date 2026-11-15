"""Everything Dagster loads: `dagster dev -m jtf.orchestration.definitions`."""

import dagster as dg

from jtf.orchestration.raw import RAW_ASSETS, raw_bucket_sensor
from jtf.orchestration.resources import RawBucket

defs = dg.Definitions(
    assets=RAW_ASSETS,
    sensors=[raw_bucket_sensor],
    resources={"raw_bucket": RawBucket(url=dg.EnvVar("JTF_RAW_URL"))},
)
