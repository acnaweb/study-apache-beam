resource "google_bigquery_dataset" "study_dataset" {
  dataset_id                  = "${var.prefix}_dataset_demo"
  friendly_name               = "${var.prefix} dataset demo"
  description                 = "Dataset for study"
  location                    = var.region
  default_table_expiration_ms = 3600000
}


resource "google_bigquery_table" "voos" {
  dataset_id = google_bigquery_dataset.study_dataset.dataset_id
  table_id   = "voos"
  deletion_protection=false

  schema = <<EOF
[
  {
    "name": "col1",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 1"
  },
  {
    "name": "col2",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 2"
  },
  {
    "name": "col3",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 3"
  },
 {
    "name": "col4",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 4"
  },
  {
    "name": "col5",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 5"
  },
  {
    "name": "col6",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 6"
  },
{
    "name": "col7",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 7"
  },
  {
    "name": "col8",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 8"
  },
  {
    "name": "col9",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 9"
  },
 {
    "name": "col10",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 10"
  },
  {
    "name": "col11",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 11"
  },
  {
    "name": "col12",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Col 12"
  }  
]
EOF

}
