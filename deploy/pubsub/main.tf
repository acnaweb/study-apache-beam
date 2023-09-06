
locals {
  subscriber_list = [
      {name: "cloudstorage-sub"}, 
      {name: "bigquery-sub"}, 
      {name: "bigtable-sub"}      
  ]
}

resource "google_pubsub_topic" "topics" {
    name     = "${var.prefix}-topic-demo"
}


resource "google_pubsub_subscription" "subscriptions" {
  for_each = { for obj in local.subscriber_list : "${obj.name}" => obj }
  name     = "${each.value.name}/"

  topic = google_pubsub_topic.topics.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false
  ack_deadline_seconds = 20

  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering    = false
}