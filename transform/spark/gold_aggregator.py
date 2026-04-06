"""
Gold Aggregator: Reads Silver Delta tables and builds Gold-layer
business aggregations ready for BI tools (Power BI, Looker, BigQuery).

Gold tables written to:
  - S3 (Athena / Redshift Spectrum)
  - GCS (BigQuery external table)
  - ADLS (Azure Synapse Analytics)
"""

import logging
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

SILVER_PATH  = os.getenv("SILVER_PATH",  "s3a://vkreddy-lakehouse-silver/delta")
GOLD_S3      = os.getenv("GOLD_S3",      "s3a://vkreddy-lakehouse-gold/delta")
GOLD_GCS     = os.getenv("GOLD_GCS",     "gs://vkreddy-lakehouse-gold-gcp/delta")
GOLD_ADLS    = os.getenv("GOLD_ADLS",    "abfss://gold@vkreddylakehouse.dfs.core.windows.net/delta")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MulticloudLakehouse-Gold")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(f"{SILVER_PATH}/{table}")


# ---------------------------------------------------------------------------
# Gold model 1: Daily revenue by product category and country
# ---------------------------------------------------------------------------
def build_daily_revenue(txn: DataFrame, cust: DataFrame, prod: DataFrame) -> DataFrame:
    return (
        txn
        .join(cust, on="customer_id", how="left")
        .join(prod, on="product_id",  how="left")
        .groupBy("txn_date", "category", "country_code")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("amount") > 500, 1).otherwise(0)).alias("high_value_orders"),
        )
        .withColumn("revenue_per_customer",
                    F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
        .withColumn("_gold_built_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Gold model 2: Customer 30/60/90 day rolling spend
# ---------------------------------------------------------------------------
def build_customer_rfm(txn: DataFrame, cust: DataFrame) -> DataFrame:
    today = F.current_date()
    return (
        txn
        .join(cust, on="customer_id", how="left")
        .groupBy("customer_id", "country_code")
        .agg(
            F.count("transaction_id").alias("total_orders"),
            F.sum("amount").alias("lifetime_spend"),
            F.max("txn_date").alias("last_order_date"),
            F.min("txn_date").alias("first_order_date"),
            F.sum(F.when(F.datediff(today, F.col("txn_date")) <= 30,  F.col("amount"))).alias("spend_30d"),
            F.sum(F.when(F.datediff(today, F.col("txn_date")) <= 60,  F.col("amount"))).alias("spend_60d"),
            F.sum(F.when(F.datediff(today, F.col("txn_date")) <= 90,  F.col("amount"))).alias("spend_90d"),
        )
        .withColumn("recency_days",  F.datediff(today, F.col("last_order_date")))
        .withColumn("rfm_segment",
                    F.when(F.col("recency_days") <= 30, "champion")
                     .when(F.col("recency_days") <= 60, "loyal")
                     .when(F.col("recency_days") <= 90, "at_risk")
                     .otherwise("churned"))
        .withColumn("_gold_built_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Gold model 3: Product performance
# ---------------------------------------------------------------------------
def build_product_performance(txn: DataFrame, prod: DataFrame) -> DataFrame:
    return (
        txn
        .join(prod, on="product_id", how="left")
        .groupBy("product_id", "category", "txn_year", "txn_month")
        .agg(
            F.count("transaction_id").alias("units_sold"),
            F.sum("amount").alias("gross_revenue"),
            F.avg("amount").alias("avg_selling_price"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
        .withColumn("_gold_built_at", F.current_timestamp())
    )


def write_gold(df: DataFrame, name: str) -> None:
    for label, base in [("s3", GOLD_S3), ("gcs", GOLD_GCS), ("adls", GOLD_ADLS)]:
        try:
            path = f"{base}/{name}"
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
            logger.info(f"Gold/{name} written to {label}: {path}")
        except Exception as e:
            logger.error(f"Failed writing Gold/{name} to {label}: {e}")


def main():
    spark = build_spark()

    customers = read_silver(spark, "customers")
    transactions = read_silver(spark, "transactions")
    products = read_silver(spark, "products")

    write_gold(build_daily_revenue(transactions, customers, products), "daily_revenue")
    write_gold(build_customer_rfm(transactions, customers),            "customer_rfm")
    write_gold(build_product_performance(transactions, products),      "product_performance")

    spark.stop()


if __name__ == "__main__":
    main()
