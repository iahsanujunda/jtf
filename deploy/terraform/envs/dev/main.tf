terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }

  # The bucket comes from bootstrap: terraform init -backend-config=bucket=PROD_PROJECT-tfstate
  backend "gcs" {
    prefix = "envs/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Dev has no capture of its own: dagster dev reads the prod raw bucket read-only and writes here.
module "warehouse" {
  source = "../../modules/warehouse"

  project_id = var.project_id
  env        = "dev"
  location   = var.region
}

# CI's identity comes from bootstrap. Here it gets what the warehouse checks on a PR need: run
# queries and create its own throwaway datasets (a dataset's creator owns it), and read ODS.
locals {
  ci_dev = "serviceAccount:ci-dev@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "ci_dev_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = local.ci_dev
}

resource "google_bigquery_dataset_iam_member" "ci_dev_reads_ods" {
  project    = var.project_id
  dataset_id = "ods"
  role       = "roles/bigquery.dataViewer"
  member     = local.ci_dev

  depends_on = [module.warehouse]
}

output "datasets" {
  value = module.warehouse.datasets
}
