variable "project_id" {
  type = string
}

variable "env" {
  description = "dev or prod; used for labels."
  type        = string
}

variable "region" {
  type    = string
  default = "asia-northeast1"
}

variable "image" {
  description = "The jtf image, pinned by digest: REGION-docker.pkg.dev/PROJECT/jtf/jtf@sha256:..."
  type        = string

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "Pin the image by digest (…@sha256:…), never by tag."
  }
}

variable "capture_groups" {
  description = "Capture group => cron schedule in Asia/Tokyo."
  type        = map(string)
  default = {
    morning = "45 8 * * *"
    jepx    = "0 11 * * *"
    evening = "0 19,22 * * *"
  }
}
