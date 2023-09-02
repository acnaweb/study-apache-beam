provider "google" {
  credentials = var.credentials
  project     = var.project_id
  region      = var.region

}

module "bucket" {
  source = "./buckets"
  name       = "acnaweb-datalake"
  project_id = var.project_id
  region   = var.region

  custom_placement_config = {
    data_locations : ["us-east1"]
  }

  iam_members = []
}