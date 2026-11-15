variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "asia-northeast1"
}

variable "image" {
  description = "The jtf image by digest. Set by the release; the first deploy passes it by hand."
  type        = string
}
