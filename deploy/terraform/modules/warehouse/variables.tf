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
