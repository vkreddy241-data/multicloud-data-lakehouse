"""
Azure ADLS Gen2 Ingestor: Reads raw data from Azure Data Lake Storage Gen2
and writes to Delta Lake bronze layer on ADLS (Azure leg of the lakehouse).

Uses Azure Data Factory triggers or direct ADB (Databricks) job invocation.
"""

import logging
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (loaded from Azure Key Vault via Databricks secret scope in prod)
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "vkreddylakehouse")
CONTAINER_RAW   = os.getenv("AZURE_CONTAINER_RAW",    "raw")
CONTAINER_BRONZE = os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
TENANT_ID       = os.getenv("AZURE_TENANT_ID",  "")
CLIENT_ID       = os.getenv("AZURE_CLIENT_ID",  "")
CLIENT_SECRET   = os.getenv("AZURE_CLIENT_SECRET", "")

ADLS_BASE       = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net"


def build_spark(app_name: str = "MulticloudLakehouse-Azure") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    # ADLS Gen2 OAuth config
    acc = "fs.azure.account"
    spark.conf.set(f"{acc}.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   "OAuth")
    spark.conf.set(f"{acc}.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"{acc}.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   CLIENT_ID)
    spark.conf.set(f"{acc}.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   CLIENT_SECRET)
    spark.conf.set(f"{acc}.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")
    return spark


def read_adls(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    full = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"
    return spark.read.format(fmt).option("header", True).load(full)


def add_metadata(df: DataFrame, source: str) -> DataFrame:
    return (
        df
        .withColumn("_source",      F.lit(source))
        .withColumn("_layer",       F.lit("bronze"))
        .withColumn("_cloud",       F.lit("azure"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_date_part",   F.to_date(F.current_timestamp()))
    )


def write_bronze(df: DataFrame, table: str) -> None:
    path = f"{ADLS_BASE}/delta/{table}"
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_date_part")
        .save(path)
    )
    logger.info(f"Written → {path}")


def ingest(spark: SparkSession, table: str, raw_path: str, fmt: str = "parquet") -> None:
    df = read_adls(spark, raw_path, fmt)
    enriched = add_metadata(df, source=f"adls://{CONTAINER_RAW}/{raw_path}")
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
