"""
Silver Transformer: Reads bronze Delta Lake tables from all 3 clouds,
applies unified schema, deduplication, and business rules,
then writes to the Silver layer (single source of truth on S3).

This is the cross-cloud unification step — bronze data from AWS, Azure,
and GCP is merged here into one consistent Silver Delta table.
"""

import logging
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source paths (bronze — one per cloud)
# ---------------------------------------------------------------------------
BRONZE_AWS   = os.getenv("BRONZE_AWS",   "s3a://vkreddy-lakehouse-bronze-aws/delta")
BRONZE_AZURE = os.getenv("BRONZE_AZURE", "abfss://bronze@vkreddylakehouse.dfs.core.windows.net/delta")
BRONZE_GCP   = os.getenv("BRONZE_GCP",  "gs://vkreddy-lakehouse-bronze-gcp/delta")

# Silver — unified on S3 (could be any cloud; S3 chosen as primary)
SILVER_PATH  = os.getenv("SILVER_PATH",  "s3a://vkreddy-lakehouse-silver/delta")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MulticloudLakehouse-Silver")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    """Union bronze data from all 3 clouds for the given table."""
    dfs = []
    for cloud, base in [("aws", BRONZE_AWS), ("azure", BRONZE_AZURE), ("gcp", BRONZE_GCP)]:
        try:
            df = spark.read.format("delta").load(f"{base}/{table}")
            dfs.append(df)
            logger.info(f"Loaded bronze/{table} from {cloud}: {df.count()} rows")
        except Exception as e:
            logger.warning(f"Could not load {cloud} bronze/{table}: {e}")
    if not dfs:
        raise RuntimeError(f"No bronze data found for table: {table}")
    return dfs[0] if len(dfs) == 1 else dfs[0].unionByName(*dfs[1:], allowMissingColumns=True)


def deduplicate(df: DataFrame, pk: str, order_col: str = "_ingested_at") -> DataFrame:
    """Keep the latest record per primary key across all cloud sources."""
    w = Window.partitionBy(pk).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_silver_rules(df: DataFrame, table: str) -> DataFrame:
    """Table-specific cleansing and standardisation rules."""
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    if table == "customers":
        df = (
            df
            .withColumn("email",        F.lower(F.trim(F.col("email"))))
            .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
            .filter(F.col("customer_id").isNotNull())
        )

    elif table == "transactions":
        df = (
            df
            .withColumn("amount",        F.round(F.col("amount").cast("double"), 2))
            .withColumn("txn_date",      F.to_date(F.col("txn_date")))
            .withColumn("txn_year",      F.year("txn_date"))
            .withColumn("txn_month",     F.month("txn_date"))
            .filter(F.col("amount") > 0)
            .filter(F.col("transaction_id").isNotNull())
        )

    elif table == "products":
        df = (
            df
            .withColumn("category",      F.lower(F.trim(F.col("category"))))
            .withColumn("price",         F.round(F.col("price").cast("double"), 2))
            .filter(F.col("product_id").isNotNull())
        )

    return df


def write_silver(df: DataFrame, table: str, pk: str) -> None:
    """Upsert into Silver Delta table."""
    path = f"{SILVER_PATH}/{table}"
    if DeltaTable.isDeltaTable(SparkSession.getActiveSession(), path):
        delta = DeltaTable.forPath(SparkSession.getActiveSession(), path)
        (
            delta.alias("tgt")
            .merge(df.alias("src"), f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Silver merge complete: {path}")
    else:
        partition_cols = ["txn_year", "txn_month"] if table == "transactions" else ["_date_part"]
        (
            df.write
            .format("delta")
            .partitionBy(*partition_cols)
            .save(path)
        )
        logger.info(f"Silver initial write: {path}")


TABLE_CONFIG = {
    "customers":    {"pk": "customer_id"},
    "transactions": {"pk": "transaction_id"},
    "products":     {"pk": "product_id"},
}


def main():
    spark = build_spark()
    for table, cfg in TABLE_CONFIG.items():
        bronze  = read_bronze(spark, table)
        deduped = deduplicate(bronze, cfg["pk"])
        silver  = apply_silver_rules(deduped, table)
        write_silver(silver, table, cfg["pk"])
    spark.stop()


if __name__ == "__main__":
    main()
