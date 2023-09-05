resource "google_pubsub_topic" "study_topic" {
    name     = "study-topic"
}

resource "google_pubsub_subscription" "study_subscription" {
  name  = "study-subscription"
  topic = google_pubsub_topic.study_topic.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = true

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = "300000.5s"
  }
  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}

resource "google_pubsub_topic" "mde_topic" {
    name     = "mde-topic"
}

locals {
  subscriber_list = [
      {name: "cloudstorage-ingestion"}, 
      {name: "bigquery-ingestion"}, 
      {name: "bigtable-ingestion"}      
  ]
}

resource "google_pubsub_subscription" "subscriptions" {
  for_each = { for obj in local.subscriber_list : "${obj.name}" => obj }
  name     = "${each.value.name}/" # Declarin

  topic = google_pubsub_topic.mde_topic.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = true

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = "300000.5s"
  }
  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}