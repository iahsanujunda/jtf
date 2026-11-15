variable "dev_project_id" {
  description = "GCP project for dev: laptop work and CI checks."
  type        = string
}

variable "prod_project_id" {
  description = "GCP project for prod. Also holds the Terraform state bucket, Artifact Registry and the GitHub identity pool."
  type        = string
}

variable "region" {
  description = "Region for every regional resource."
  type        = string
  default     = "asia-northeast1"
}

variable "github_repository" {
  description = "GitHub repository allowed to reach GCP, as owner/name."
  type        = string
}

variable "billing_account_id" {
  description = "Billing account that pays for both projects. Kept out of git."
  type        = string
}

variable "monthly_budget_jpy" {
  description = "Monthly budget in yen; alerts at 75%, 100% and on forecast spend."
  type        = number
  default     = 12000
}
