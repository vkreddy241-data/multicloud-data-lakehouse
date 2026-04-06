terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "vkreddy-tf-state"
    key    = "multicloud-lakehouse/aws/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" { region = var.aws_region }

locals {
  tags = { Project = "multicloud-lakehouse", Owner = "vikas-reddy", ManagedBy = "terraform" }
}

# ---------------------------------------------------------------------------
# S3 Lakehouse buckets
# ---------------------------------------------------------------------------
locals {
  buckets = toset([
    "vkreddy-lakehouse-raw-aws",
    "vkreddy-lakehouse-bronze-aws",
    "vkreddy-lakehouse-silver",
    "vkreddy-lakehouse-gold",
    "vkreddy-lakehouse-scripts",
  ])
}

resource "aws_s3_bucket" "lakehouse" {
  for_each      = local.buckets
  bucket        = each.key
  force_destroy = false
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = "vkreddy-lakehouse-silver"
  versioning_configuration { status = "Enabled" }
  depends_on = [aws_s3_bucket.lakehouse]
}

# ---------------------------------------------------------------------------
# EMR cluster for Spark jobs (Silver + Gold + Optimize)
# ---------------------------------------------------------------------------
resource "aws_emr_cluster" "lakehouse" {
  name          = "multicloud-lakehouse-spark"
  release_label = "emr-6.13.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id        = var.private_subnet_id
    instance_profile = aws_iam_instance_profile.emr.arn
  }

  master_instance_group { instance_type = "m5.xlarge" }

  core_instance_group {
    instance_type  = "m5.2xlarge"
    instance_count = 3
    ebs_config {
      size                 = 200
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.extensions"          = "io.delta.sql.DeltaSparkSessionExtension"
        "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark.jars.packages"           = "io.delta:delta-core_2.12:2.4.0"
      }
    }
  ])

  log_uri      = "s3://vkreddy-lakehouse-scripts/emr-logs/"
  service_role = aws_iam_role.emr_service.arn
  tags         = local.tags
}

# ---------------------------------------------------------------------------
# Glue Catalog (Hive Metastore for Delta tables)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "bronze" {
  name = "lakehouse_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "lakehouse_silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "lakehouse_gold"
}
