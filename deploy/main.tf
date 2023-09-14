provider "google" {
  credentials = var.credentials
  project     = var.project_id
  region      = var.region

}

module "bucket" {
  source = "./bucket"
  prefix  = var.prefix
  project_id = var.project_id
  region   = var.region
}

module "pubsub" {
  source = "./pubsub"  
  prefix  = var.prefix
}

module "bigquery" {
  source = "./bigquery"  
  region   = var.region
  prefix  = var.prefix
}

module "bigtable" {
  source = "./bigtable"  
  region   = var.region
  prefix  = var.prefix
}



