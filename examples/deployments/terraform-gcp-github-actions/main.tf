terraform {
  backend "gcs" {
    bucket = "<gcp_bucket_for_saving_state_file>"
    prefix = "terraform/state/chroma-db-server"
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

