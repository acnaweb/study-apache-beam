
locals {
  subscriber_list = [
      {name: "${var.prefix}-cloudstorage-sub"}, 
      {name: "${var.prefix}-bigquery-sub"}, 
      {name: "${var.prefix}-bigtable-sub"}      
  ]
}

resource "google_pubsub_topic" "topics" {
    name     = "${var.prefix}-topic"
}

resource "google_pubsub_subscription" "subscriptions" {
  for_each = { for obj in local.subscriber_list : "${obj.name}" => obj }
  name     = "${each.value.name}/"

  topic = google_pubsub_topic.topics.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false
  ack_deadline_seconds = 20

  enable_message_ordering    = false
}


