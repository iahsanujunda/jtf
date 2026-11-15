output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "capture_jobs" {
  value = { for key, job in google_cloud_run_v2_job.capture : key => job.name }
}

output "healthcheck_ping_key_secret" {
  description = "Add the value with: gcloud secrets versions add SECRET --data-file=-"
  value       = google_secret_manager_secret.healthcheck_ping_key.secret_id
}
