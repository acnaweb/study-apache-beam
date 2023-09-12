resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "${var.prefix}_dataset"
  friendly_name               = "${var.prefix} dataset"
  location                    = var.region
  default_table_expiration_ms = 3600000
}

