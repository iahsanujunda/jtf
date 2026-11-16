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

variable "capture_sources" {
  description = "Scheduled capture jobs. Key => cron schedule (Asia/Tokyo), Cloud Run task retries, and optionally the source (defaults to the key) and extra jtf arguments. Source names match `jtf sources`."
  type = map(object({
    schedule     = string
    task_retries = number
    source       = optional(string)
    extra_args   = optional(list(string), [])
  }))
  default = {
    tepco_actuals                = { schedule = "45 8 * * *", task_retries = 1 }
    tepco_area_forecast          = { schedule = "45 8 * * *", task_retries = 1 }
    tepco_area_bg_plan           = { schedule = "45 8 * * *", task_retries = 1 }
    tepco_area_actuals_today     = { schedule = "45 8 * * *", task_retries = 1 }
    tepco_demand_outlook         = { schedule = "45 8 * * *", task_retries = 1 }
    tepco_area_forecast_next_day = { schedule = "0 19,22 * * *", task_retries = 1 }
    tepco_area_bg_plan_next_day  = { schedule = "0 19,22 * * *", task_retries = 1 }
    # Connections from Cloud Run to Open-Meteo sometimes fail to open; a fresh container succeeds.
    openmeteo_live    = { schedule = "45 8 * * *", task_retries = 3 }
    jepx_spot         = { schedule = "0 11 * * *", task_retries = 1 }
    meti_fit_capacity = { schedule = "45 8 * * *", task_retries = 1 }
    # TEPCO rewrites old months, occasionally in bulk; check every monthly file once a month.
    tepco_actuals_recapture = {
      source       = "tepco_actuals"
      extra_args   = ["--recapture"]
      schedule     = "30 9 1 * *"
      task_retries = 1
    }
    # The demand outlook's archive zips are rewritten for months too.
    tepco_demand_outlook_recapture = {
      source       = "tepco_demand_outlook"
      extra_args   = ["--recapture"]
      schedule     = "40 9 1 * *"
      task_retries = 1
    }
  }
}
