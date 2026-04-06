"""
AWS S3 Ingestor: Reads raw data from S3 and writes to the Delta Lake
bronze layer on S3 (us-east-1). Supports CSV, JSON, Parquet.

Entry point for the AWS leg of the multi-cloud lakehouse.
"""

import logging
import os
from datetime import datetime
from typing import Optional

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
AWS_REGION      = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET      = os.getenv("AWS_RAW_BUCKET",    "vkreddy-lakehouse-raw-aws")
BRONZE_BUCKET   = os.getenv("AWS_BRONZE_BUCKET", "vkreddy-lakehouse-bronze-aws")
BRONZE_PATH     = f"s3a://{BRONZE_BUCKET}/delta"


def build_spark(app_name: str = "MulticloudLakehouse-AWS") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",          "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",        "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


def list_new_files(bucket: str, prefix: str, since: Optional[datetime] = None) -> list:
    """Return S3 keys modified after `since`."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if since is None or obj["LastModified"].replace(tzinfo=None) > since:
                keys.append(f"s3a://{bucket}/{obj['Key']}")
    return keys


def read_source(spark: SparkSession, paths: list, fmt: str = "csv") -> DataFrame:
    reader = spark.read
    if fmt == "csv":
        return reader.option("header", True).option("inferSchema", True).csv(paths)
    if fmt == "json":
        return reader.json(paths)
    if fmt == "parquet":
        return reader.parquet(paths)
    raise ValueError(f"Unsupported format: {fmt}")


def add_metadata(df: DataFrame, source: str, layer: str = "bronze") -> DataFrame:
    return (
        df
        .withColumn("_source",       F.lit(source))
        .withColumn("_layer",        F.lit(layer))
        .withColumn("_cloud",        F.lit("aws"))
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_date_part",    F.to_date(F.current_timestamp()))
    )


def write_bronze(df: DataFrame, table: str) -> None:
    path = f"{BRONZE_PATH}/{table}"
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_date_part")
        .save(path)
    )
    logger.info(f"Written {df.count()} rows → {path}")


def ingest(spark: SparkSession, table: str, s3_prefix: str, fmt: str = "csv") -> None:
    files = list_new_files(RAW_BUCKET, s3_prefix)
    if not files:
        logger.info(f"No new files for {table}. Skipping.")
        return
    raw = read_source(spark, files, fmt)
    enriched = add_metadata(raw, source=f"s3://{RAW_BUCKET}/{s3_prefix}")
    write_bronze(enriched, table)


def main():
    spark = build_spark()
    tables = [
        ("customers",    "raw/customers/",    "csv"),
        ("transactions", "raw/transactions/", "parquet"),
        ("products",     "raw/products/",     "json"),
    ]
    for table, prefix, fmt in tables:
        ingest(spark, table, prefix, fmt)
    spark.stop()


if __name__ == "__main__":
    main()
