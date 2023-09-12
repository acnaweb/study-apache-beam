
locals {
  prefix = "mde"
  mde_subscriber_list = [
      {name: "${local.prefix}-cloudstorage-sub"}, 
      {name: "${local.prefix}-bigquery-sub"}, 
      {name: "${local.prefix}-bigtable-sub"}      
  ]
}

resource "google_pubsub_topic" "mde_topic" {
    name     = "${local.prefix}-topic"
}

resource "google_pubsub_subscription" "mde_subscriptions" {
  for_each = { for obj in local.mde_subscriber_list : "${obj.name}" => obj }
  name     = "${each.value.name}/"

  topic = google_pubsub_topic.mde_topic.name

  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = false
  ack_deadline_seconds = 20

  enable_message_ordering    = false
}


