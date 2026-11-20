variable "project_id" {
  type = string
}

variable "env" {
  description = "dev or prod; used for labels."
  type        = string
}

variable "location" {
  type    = string
  default = "asia-northeast1"
}

variable "datasets" {
  description = "Dataset ID => description."
  type        = map(string)
  default = {
    ods = "Raw files as tables: the source's own values and labels, with lineage on every row"
  }
}

variable "dbt_prefix" {
  description = <<-EOT
    The prefix of dbt's datasets, <prefix>_<layer>, to declare them here beside ODS. Null leaves
    them to dbt, which creates a dataset the first time it builds into one: right for dev, where
    every developer and every CI run has a prefix of its own.
  EOT
  type        = string
  default     = null
}
