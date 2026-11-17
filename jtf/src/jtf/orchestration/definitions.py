"""Everything Dagster loads: `dagster dev -m jtf.orchestration.definitions`."""

import dagster as dg

from jtf.orchestration.dbt import DBT, dbt_models
from jtf.orchestration.ml import ML_ASSETS
from jtf.orchestration.ods import MANIFEST_SPEC, ODS_ASSETS
from jtf.orchestration.raw import RAW_ASSETS, raw_bucket_sensor
from jtf.orchestration.resources import BigQuery, Datasets, RawBucket

# Evaluates every asset's automation condition, e.g. "parse a day when its raw files change".
# Dagster's default one starts stopped; declaring it here keeps it running in every deployment.
automation = dg.AutomationConditionSensorDefinition(
    "automation",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[*RAW_ASSETS, *ODS_ASSETS, MANIFEST_SPEC, dbt_models, *ML_ASSETS],
    sensors=[raw_bucket_sensor, automation],
    resources={
        "raw_bucket": RawBucket(url=dg.EnvVar("JTF_RAW_URL")),
        "bigquery": BigQuery(project=dg.EnvVar("JTF_BIGQUERY_PROJECT")),
        "datasets": Datasets(
            project=dg.EnvVar("JTF_BIGQUERY_PROJECT"),
            location=dg.EnvVar("JTF_BIGQUERY_LOCATION"),
            prefix=dg.EnvVar("JTF_DBT_DATASET_PREFIX"),
        ),
        "dbt": DBT,
    },
)
