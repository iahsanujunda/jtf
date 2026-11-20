terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 6.0"
    }
  }

  # The bucket comes from bootstrap: terraform init -backend-config=bucket=PROD_PROJECT-tfstate
  backend "gcs" {
    prefix = "envs/prod"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "forecast_env" {
  source = "../../modules/forecast_env"

  project_id = var.project_id
  env        = "prod"
  region     = var.region
  image      = var.image
}

# The warehouse: everything built from the raw files, and every recorded run. Written only by the
# capture jobs' orchestrator: Dagster running from a clean checkout of main. Dev holds nothing that
# prod needs; it is where a change is built and tried before it merges.
#
# The datasets are declared here, not left to dbt to create, so that they carry the project's
# labels and keep their tables if this configuration is ever destroyed.
module "warehouse" {
  source = "../../modules/warehouse"

  project_id = var.project_id
  env        = "prod"
  location   = var.region
  dbt_prefix = "prod"
}

# What a trained model leaves behind: each month's model file and the rows it learned from, as
# MLflow artifacts. The record of a run and its scores are in the warehouse; these are the files.
resource "google_storage_bucket" "models" {
  project                     = var.project_id
  name                        = "${var.project_id}-models"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = { app = "jtf", env = "prod" }

  soft_delete_policy {
    retention_duration_seconds = 7776000
  }

  lifecycle {
    prevent_destroy = true
  }
}

# A cap on what queries may scan per day, as in dev: a runaway loop can't run up a bill. A full
# rebuild is about 5 GB and a day's runs a few. In MiB, BigQuery's unit.
provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_service_usage_consumer_quota_override" "bigquery_query_per_day" {
  provider       = google-beta
  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project")
  override_value = "204800" # 200 GiB
  force          = true     # lowering a limit by more than 10% needs it
}

resource "google_service_usage_consumer_quota_override" "bigquery_query_per_user_per_day" {
  provider       = google-beta
  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project/user")
  override_value = "102400" # 100 GiB
  force          = true
}

output "datasets" {
  value = module.warehouse.datasets
}

output "models_bucket" {
  value = google_storage_bucket.models.name
}

output "raw_bucket" {
  value = module.forecast_env.raw_bucket
}

output "capture_jobs" {
  value = module.forecast_env.capture_jobs
}

output "healthcheck_ping_key_secret" {
  value = module.forecast_env.healthcheck_ping_key_secret
}
