# bootstrap

The one step that must run before CI can deploy anything. Run once per account from a laptop with your own gcloud login (`make bootstrap`); it starts with local state, since the state bucket doesn't exist yet, and moves its state into that bucket at the end of the first run, so nothing depends on one laptop.

| Creates | Why it can't come from CI |
| --- | --- |
| Required APIs on both projects | Nothing else works without them |
| Terraform state bucket | Every other Terraform root stores its state there |
| Workload Identity Federation pool and provider, restricted to your GitHub repo | CI needs it to reach GCP at all |
| CI service accounts: one for PRs (dev), one for releases (prod) | CI runs as these |
| Monthly budget on your billing account | Cost alerts before anything billable exists |

Inputs come from `terraform.tfvars` (gitignored), copied from `terraform.tfvars.example`; a `make init` wizard to write it comes in phase 5. When it finishes, it sets the GitHub repository variables CI needs if the `gh` CLI is logged in, or prints them.
