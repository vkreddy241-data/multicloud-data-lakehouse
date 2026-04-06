"""
GCP GCS Ingestor: Reads raw data from Google Cloud Storage and writes
to Delta Lake bronze layer on GCS (GCP leg of the lakehouse).

Runs on Dataproc or Cloud Composer-triggered Spark job.
"""

import logging
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

GCS_RAW_BUCKET    = os.getenv("GCS_RAW_BUCKET",    "vkreddy-lakehouse-raw-gcp")
GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "vkreddy-lakehouse-bronze-gcp")
GCS_BRONZE_PATH   = f"gs://{GCS_BRONZE_BUCKET}/delta"
GCP_PROJECT       = os.getenv("GCP_PROJECT", "vkreddy-data-platform")


def build_spark(app_name: str = "MulticloudLakehouse-GCP") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )


def read_gcs(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    full = f"gs://{GCS_RAW_BUCKET}/{path}"
    return spark.read.format(fmt).option("header", True).load(full)


def add_metadata(df: DataFrame, source: str) -> DataFrame:
    return (
        df
        .withColumn("_source",      F.lit(source))
        .withColumn("_layer",       F.lit("bronze"))
        .withColumn("_cloud",       F.lit("gcp"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_date_part",   F.to_date(F.current_timestamp()))
    )


def write_bronze(df: DataFrame, table: str) -> None:
    path = f"{GCS_BRONZE_PATH}/{table}"
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_date_part")
        .save(path)
    )
    logger.info(f"Written → {path}")


def ingest(spark: SparkSession, table: str, gcs_path: str, fmt: str = "parquet") -> None:
    df = read_gcs(spark, gcs_path, fmt)
    enriched = add_metadata(df, source=f"gs://{GCS_RAW_BUCKET}/{gcs_path}")
    write_bronze(enriched, table)


def main():
    spark = build_spark()
    tables = [
        ("customers",    "customers/",    "csv"),
        ("transactions", "transactions/", "parquet"),
        ("products",     "products/",     "json"),
    ]
    for table, path, fmt in tables:
        ingest(spark, table, path, fmt)
    spark.stop()


if __name__ == "__main__":
    main()
