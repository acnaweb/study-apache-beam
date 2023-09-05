resource "google_pubsub_topic" "petrobras_topic" {
    name     = "petrobras-topic"
}

resource "google_pubsub_subscription" "petrobras1-subscription" {
  name  = "petrobras1-subscription"
  topic = google_pubsub_topic.petrobras_topic.name

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