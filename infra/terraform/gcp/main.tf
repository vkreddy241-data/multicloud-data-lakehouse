terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 5.0" }
  }
  backend "gcs" {
    bucket = "vkreddy-tf-state-gcp"
    prefix = "multicloud-lakehouse/gcp"
  }
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

locals {
  labels = { project = "multicloud-lakehouse", owner = "vikas-reddy", managed_by = "terraform" }
}

# ---------------------------------------------------------------------------
# GCS buckets
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "raw" {
  name          = "vkreddy-lakehouse-raw-gcp"
  location      = var.gcp_region
  storage_class = "STANDARD"
  force_destroy = false
  labels        = local.labels

  lifecycle_rule {
    action { type = "SetStorageClass"; storage_class = "NEARLINE" }
    condition { age = 90 }
  }
}

resource "google_storage_bucket" "bronze" {
  name          = "vkreddy-lakehouse-bronze-gcp"
  location      = var.gcp_region
  storage_class = "STANDARD"
  force_destroy = false
  labels        = local.labels
}

resource "google_storage_bucket" "gold" {
  name          = "vkreddy-lakehouse-gold-gcp"
  location      = var.gcp_region
  storage_class = "STANDARD"
  force_destroy = false
  labels        = local.labels
}

# ---------------------------------------------------------------------------
# Dataproc cluster for Spark/Delta jobs on GCP
# ---------------------------------------------------------------------------
resource "google_dataproc_cluster" "lakehouse" {
  name    = "lakehouse-cluster"
  project = var.gcp_project
  region  = var.gcp_region
  labels  = local.labels

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config { boot_disk_size_gb = 100 }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-8"
      disk_config { boot_disk_size_gb = 200 }
    }

    software_config {
      image_version = "2.1-debian11"
      properties = {
        "spark:spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
        "spark:spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      }
    }

    gce_cluster_config {
      zone = "${var.gcp_region}-a"
    }
  }
}

# ---------------------------------------------------------------------------
# BigQuery dataset for Gold layer external tables
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "gold" {
  dataset_id    = "lakehouse_gold"
  friendly_name = "Multicloud Lakehouse Gold"
  location      = var.gcp_region
  project       = var.gcp_project
  labels        = local.labels
}

resource "google_bigquery_table" "daily_revenue" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "daily_revenue"
  project    = var.gcp_project

  external_data_configuration {
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.gold.name}/delta/daily_revenue/*.parquet"]
    autodetect    = true
  }
}
