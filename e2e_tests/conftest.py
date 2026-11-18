"""The e2e tests' world (README.md, The test world): the raw files of `world/raw` parsed into a
throwaway prefix's ODS, then built by dbt with the window's variables. Built once per session and
dropped after it. The tests plant files into it, so each compares what it needs before and after
its own plant.

Needs `JTF_BIGQUERY_PROJECT`, `JTF_BIGQUERY_LOCATION` and a CI prefix in `JTF_DBT_DATASET_PREFIX`
(`ci_<run id>[_<attempt>]_e2e`): `make e2e` sets one.
"""

import os

import pytest
from harness import WORLD, World

from jtf import ci


@pytest.fixture(scope="session")
def world(tmp_path_factory) -> World:
    try:
        project, location = os.environ["JTF_BIGQUERY_PROJECT"], os.environ["JTF_BIGQUERY_LOCATION"]
        prefix = ci.ci_prefix(os.environ["JTF_DBT_DATASET_PREFIX"])
    except (KeyError, ValueError) as e:
        pytest.exit(f"the e2e tests need BigQuery and a CI prefix ({e}): run `make e2e`", 2)
    world = World(project, location, prefix, tmp_path_factory.mktemp("dbt-target"))
    ci.create(world.client, project, location, prefix)
    try:
        world.parse(WORLD, batch_id="e2e-world")
        world.dbt("seed")
        world.dbt("run")
        yield world
    finally:
        ci.drop(world.client, project, prefix)
