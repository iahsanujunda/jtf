locals {
  labels = { app = "jtf" }

  projects = {
    dev  = var.dev_project_id
    prod = var.prod_project_id
  }

  common_apis = [
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com",
  ]
  apis = {
    dev = local.common_apis
    prod = concat(local.common_apis, [
      "artifactregistry.googleapis.com",
      "billingbudgets.googleapis.com",
      "cloudscheduler.googleapis.com",
      "run.googleapis.com",
      "secretmanager.googleapis.com",
    ])
  }
  project_apis = merge([
    for env, apis in local.apis : {
      for api in apis : "${env}/${api}" => { project = local.projects[env], api = api }
    }
  ]...)
}

resource "google_project_service" "apis" {
  for_each = local.project_apis

  project            = each.value.project
  service            = each.value.api
  disable_on_destroy = false
}

# Every other Terraform root keeps its state here, one prefix per root.
resource "google_storage_bucket" "tfstate" {
  project                     = var.prod_project_id
  name                        = "${var.prod_project_id}-tfstate"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.apis]
}

# Images must be pushable before any job can reference one, so the registry lives here.
resource "google_artifact_registry_repository" "jtf" {
  project       = var.prod_project_id
  location      = var.region
  repository_id = "jtf"
  format        = "DOCKER"
  description   = "The jtf image, deployed by digest to every environment."
  labels        = local.labels

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  cleanup_policies {
    id     = "delete-older-than-30-days"
    action = "DELETE"
    condition {
      older_than = "2592000s"
    }
  }

  depends_on = [google_project_service.apis]
}

# GitHub Actions reaches GCP through Workload Identity Federation: no service account keys.
resource "google_iam_workload_identity_pool" "github" {
  project                   = var.prod_project_id
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.prod_project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"        = "assertion.sub"
    "attribute.repository"  = "assertion.repository"
    "attribute.environment" = "assertion.environment"
    # The workflow file that is running, without the branch or pull request it runs for:
    # owner/name/.github/workflows/ci.yml@refs/pull/16/merge becomes owner/name/.github/workflows/ci.yml
    "attribute.workflow" = "assertion.job_workflow_ref.extract('{workflow}@')"
  }
  # Only this repository can use the pool at all.
  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "ci_dev" {
  project      = var.dev_project_id
  account_id   = "ci-dev"
  display_name = "CI for pull requests"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "ci_release" {
  project      = var.prod_project_id
  account_id   = "ci-release"
  display_name = "CI for releases to prod"

  depends_on = [google_project_service.apis]
}

# Only the repository's CI workflow may act as the dev CI account, not any other workflow file. On a
# pull request GitHub runs the PR's own copy of ci.yml, so this pins the file, not its contents;
# pull requests from forks get no identity token at all.
resource "google_service_account_iam_member" "ci_dev_from_github" {
  service_account_id = google_service_account.ci_dev.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.workflow/${var.github_repository}/.github/workflows/ci.yml"
}

# Only jobs in the repository's "prod" GitHub environment, which requires approval, may act as the release account.
resource "google_service_account_iam_member" "ci_release_from_github_prod_environment" {
  service_account_id = google_service_account.ci_release.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.environment/prod"
}

resource "google_billing_budget" "monthly" {
  provider        = google.billing
  billing_account = var.billing_account_id
  display_name    = "JTF monthly"

  amount {
    specified_amount {
      currency_code = "JPY"
      units         = tostring(var.monthly_budget_jpy)
    }
  }

  threshold_rules {
    threshold_percent = 0.75
  }
  threshold_rules {
    threshold_percent = 1.0
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  depends_on = [google_project_service.apis]
}
