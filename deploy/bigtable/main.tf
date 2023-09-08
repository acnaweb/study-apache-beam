resource "google_bigtable_instance" "bigtable-instance" {
  name = "${var.prefix}-bigtable-instance"
  deletion_protection = false

  cluster {
    cluster_id = "${var.prefix}-bigtable-cluster"    
    storage_type = "HDD"
    zone = "${var.region}-a"

    autoscaling_config {
      min_nodes = 1
      max_nodes = 1
      cpu_target = 10
    }
  }   
}