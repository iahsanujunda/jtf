"""dbt's models and seeds as assets, with dbt's data tests as their checks.

The SQL lives in warehouse/dbt; this module only tells Dagster about it. dagster-dbt's default keys
already fit: a source is `ods/<table>`, so it lands on the ODS asset and lineage runs from each raw
file to every model; a model or seed is `<schema>/<name>`, such as `dwd/dwd_tepco_actuals`.
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_DIR = Path(__file__).resolve().parents[4] / "warehouse" / "dbt"
DBT_PROJECT = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)

# `dagster dev` re-parses the project on every load, so an edited model shows at once. Anywhere
# else (pytest, CI) the manifest is made once if it is missing; parsing reads no data.
DBT_PROJECT.prepare_if_dev()
if not DBT_PROJECT.manifest_path.exists():
    DBT_PROJECT.preparer.prepare(DBT_PROJECT)

AC = dg.AutomationCondition

# A model rebuilds when any of its inputs updates. Not `eager()`: an unpartitioned model under the
# day-partitioned ODS assets would wait until every day had been parsed, so a single failed
# capture would stop it for good. Nor while an input's own inputs are being built: a parse reports
# the manifest from its first step, and on 2026-11-17 the decision-time spine rebuilt then, ahead
# of the DWD it is joined with, so D−2 was incomplete for a minute.
REBUILD_ON_UPDATE = (
    AC.any_deps_updated().since_last_handled()
    & ~AC.any_deps_in_progress()
    & ~AC.any_deps_match(AC.any_deps_in_progress())
    & ~AC.in_progress()
).with_label("rebuild_on_update")


class Translator(DagsterDbtTranslator):
    """Groups by layer (`dim`, `dwd`, `dws`); models rebuild by themselves, seeds only by hand."""

    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        if props["resource_type"] == "model":
            return spec.replace_attributes(
                group_name=props["config"]["schema"], automation_condition=REBUILD_ON_UPDATE
            )
        if props["resource_type"] == "seed":
            return spec.replace_attributes(group_name=props["config"]["schema"])
        return spec


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    dagster_dbt_translator=Translator(),
)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """`dbt build` of what was selected: models and seeds with their tests, in dbt's order."""
    yield from dbt.cli(["build"], context=context).stream()


DBT = DbtCliResource(project_dir=DBT_PROJECT)
