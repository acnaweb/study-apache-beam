resource "google_pubsub_topic" "study_topic" {
    name     = "petro-study-topic"
}

resource "google_pubsub_subscription" "study_subscription" {
  name  = "petro-study-sub"
  topic = google_pubsub_topic.study_topic.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false

  ack_deadline_seconds = 20

  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}

