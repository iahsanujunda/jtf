# BigQuery datasets, one per warehouse layer. The tables inside are created by the code that
# writes them (the ODS loader, dbt), so a schema change ships with the change that needs it.
resource "google_bigquery_dataset" "layer" {
  for_each = var.datasets

  project     = var.project_id
  dataset_id  = each.key
  location    = var.location
  description = each.value
  labels      = { app = "jtf", env = var.env }

  # Dev can be rebuilt from the raw files at any time; prod keeps its tables.
  delete_contents_on_destroy = var.env != "prod"
}
