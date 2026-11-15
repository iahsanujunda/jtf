output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "capture_jobs" {
  value = { for group, job in google_cloud_run_v2_job.capture : group => job.name }
}

output "healthcheck_secrets" {
  description = "Add each value with: gcloud secrets versions add SECRET --data-file=-"
  value       = { for group, secret in google_secret_manager_secret.healthcheck : group => secret.secret_id }
}
