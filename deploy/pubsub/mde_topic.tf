
locals {
  subscriber_list = [
      {name: "cloudstorage-ingestion-sub"}, 
      {name: "bigquery-ingestion-sub"}, 
      {name: "bigtable-ingestion-sub"}      
  ]
}

resource "google_pubsub_topic" "mde_topic" {
    name     = "petrobras-mde-topic"
}


resource "google_pubsub_subscription" "subscriptions" {
  for_each = { for obj in local.subscriber_list : "${obj.name}" => obj }
  name     = "${each.value.name}/" # Declarin

  topic = google_pubsub_topic.mde_topic.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false
  ack_deadline_seconds = 20

  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}