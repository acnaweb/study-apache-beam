locals {
  folder_list = [
      {folder: "inputs/"}, 
      {folder: "outputs/"}, 
      {folder: "temp/"}, 
      {folder: "template/"}, 
      {folder: "pipelines/"},       
  ]
}

resource "google_storage_bucket" "bucket" {
  name                        = "${var.prefix}-datalake"
  project                     = var.project_id
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.bucket_policy_only
  labels                      = var.labels
  force_destroy               = var.force_destroy
  public_access_prevention    = var.public_access_prevention

  versioning {
    enabled = var.versioning
  }

  dynamic "retention_policy" {
    for_each = var.retention_policy == null ? [] : [var.retention_policy]
    content {
      is_locked        = var.retention_policy.is_locked
      retention_period = var.retention_policy.retention_period
    }
  }

  dynamic "encryption" {
    for_each = var.encryption == null ? [] : [var.encryption]
    content {
      default_kms_key_name = var.encryption.default_kms_key_name
    }
  }

  dynamic "logging" {
    for_each = var.log_bucket == null ? [] : [var.log_bucket]
    content {
      log_bucket        = var.log_bucket
      log_object_prefix = var.log_object_prefix
    }
  }
}

resource "google_storage_bucket_object" "folders" {
  for_each = { for obj in local.folder_list : "${obj.folder}" => obj }
  bucket = google_storage_bucket.bucket.name
  name     = "${each.value.folder}/" # Declaring an object with a trailing '/' creates a directory
  content  = "  "    
}
