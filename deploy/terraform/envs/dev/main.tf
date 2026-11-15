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

output "datasets" {
  value = module.warehouse.datasets
}
