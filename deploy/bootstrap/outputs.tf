output "tfstate_bucket" {
  description = "Bucket for every other root's state; pass it with -backend-config=bucket=..."
  value       = google_storage_bucket.tfstate.name
}

output "image_repository" {
  description = "Where images are pushed."
  value       = "${var.region}-docker.pkg.dev/${var.prod_project_id}/${google_artifact_registry_repository.jtf.repository_id}"
}

output "workload_identity_provider" {
  description = "GitHub repository variable GCP_WORKLOAD_IDENTITY_PROVIDER."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "ci_dev_service_account" {
  description = "GitHub repository variable GCP_CI_DEV_SERVICE_ACCOUNT."
  value       = google_service_account.ci_dev.email
}

output "ci_release_service_account" {
  description = "GitHub repository variable GCP_CI_RELEASE_SERVICE_ACCOUNT."
  value       = google_service_account.ci_release.email
}
