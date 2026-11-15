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

output "raw_bucket" {
  value = module.forecast_env.raw_bucket
}

output "capture_jobs" {
  value = module.forecast_env.capture_jobs
}

output "healthcheck_secrets" {
  value = module.forecast_env.healthcheck_secrets
}
