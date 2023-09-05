provider "google" {
  credentials = var.credentials
  project     = var.project_id
  region      = var.region

}

module "bucket" {
  source = "./bucket"
  name       = var.bucket_name
  project_id = var.project_id
  region   = var.region
}

module "pubsub" {
  source = "./pubsub"  
}


# module "compute" {
#   source = "./compute"  
# }