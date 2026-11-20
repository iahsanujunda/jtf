# BigQuery datasets, one per warehouse layer. The tables inside are created by the code that
# writes them (the ODS loader, dbt), so a schema change ships with the change that needs it.
locals {
  # dbt's layers (warehouse/dbt/dbt_project.yml), declared when the environment names a prefix
  layers = {
    dim = "What nothing is learned about over time: the sun, the weather points, the parts of the area"
    dwd = "Facts with every version and when each became known: MW, slots by their start in JST"
    dws = "One version per decision time: features, read from DWD only as of that time"
    ads = "Outputs: labels, predictions, outcomes and the record of every run"
  }
  dbt_datasets = var.dbt_prefix == null ? {} : {
    for layer, description in local.layers : "${var.dbt_prefix}_${layer}" => description
  }
}

resource "google_bigquery_dataset" "layer" {
  for_each = merge(var.datasets, local.dbt_datasets)

  project     = var.project_id
  dataset_id  = each.key
  location    = var.location
  description = each.value
  labels      = { app = "jtf", env = var.env }

  # Dev can be rebuilt from the raw files at any time; prod keeps its tables.
  delete_contents_on_destroy = var.env != "prod"
}
