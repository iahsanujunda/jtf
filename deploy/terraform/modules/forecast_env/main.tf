# One environment's capture slice: the raw bucket, one Cloud Run job per capture group,
# and the schedules that start them. Capture deliberately depends on nothing else.

locals {
  labels = { app = "jtf", env = var.env }
}

resource "google_storage_bucket" "raw" {
  project                     = var.project_id
  name                        = "${var.project_id}-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels

  soft_delete_policy {
    retention_duration_seconds = 604800
  }
}

resource "google_service_account" "fetch" {
  project      = var.project_id
  account_id   = "jtf-fetch"
  display_name = "Capture job: writes raw files"
}

# objectCreator can create objects but not delete or overwrite them:
# raw files stay create-only even if the code has a bug.
resource "google_storage_bucket_iam_member" "fetch_creates_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.fetch.email}"
}

# Containers only. Values are added with `gcloud secrets versions add`, so they never
# appear in git or in Terraform state.
resource "google_secret_manager_secret" "healthcheck" {
  for_each = var.capture_groups

  project   = var.project_id
  secret_id = "jtf-healthcheck-${each.key}"
  labels    = local.labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "fetch_reads_healthcheck" {
  for_each = var.capture_groups

  project   = var.project_id
  secret_id = google_secret_manager_secret.healthcheck[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.fetch.email}"
}

resource "google_cloud_run_v2_job" "capture" {
  for_each = var.capture_groups

  project             = var.project_id
  name                = "jtf-fetch-${each.key}"
  location            = var.region
  deletion_protection = false
  labels              = local.labels

  template {
    task_count = 1

    template {
      service_account = google_service_account.fetch.email
      timeout         = "600s"
      max_retries     = 1

      containers {
        image = var.image
        args  = ["fetch", "--group", each.key]

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "JTF_RAW_URL"
          value = "gs://${google_storage_bucket.raw.name}"
        }
        env {
          name  = "JTF_IMAGE_DIGEST"
          value = var.image
        }
        env {
          name = "JTF_HEALTHCHECK_URL_${upper(each.key)}"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.healthcheck[each.key].secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.fetch_reads_healthcheck]
}

resource "google_service_account" "scheduler" {
  project      = var.project_id
  account_id   = "jtf-scheduler"
  display_name = "Cloud Scheduler: starts the capture jobs"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_runs_capture" {
  for_each = var.capture_groups

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.capture[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "capture" {
  for_each = var.capture_groups

  project          = var.project_id
  region           = var.region
  name             = "jtf-fetch-${each.key}"
  schedule         = each.value
  time_zone        = "Asia/Tokyo"
  attempt_deadline = "60s"

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.capture[each.key].id}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_runs_capture]
}
