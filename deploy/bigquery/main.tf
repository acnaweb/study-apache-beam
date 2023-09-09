resource "google_bigquery_dataset" "study_dataset" {
  dataset_id                  = "${var.prefix}_dataset_demo"
  friendly_name               = "${var.prefix} dataset demo"
  description                 = "Dataset for study"
  location                    = var.region
  default_table_expiration_ms = 3600000
}

