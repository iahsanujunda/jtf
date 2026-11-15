terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

provider "google" {
  region = var.region
}

# Budget calls need a quota project; only this alias sets one.
provider "google" {
  alias                 = "billing"
  user_project_override = true
  billing_project       = var.prod_project_id
}
