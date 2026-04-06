"""
Delta Lake maintenance: OPTIMIZE + VACUUM + Z-ORDER across all layers
and all clouds. Run daily via Airflow.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

VACUUM_HOURS = 168   # 7 days


@dataclass
class DeltaTableConfig:
    path: str
    zorder_cols: Optional[list]
    label: str


TABLES = [
    # Bronze
    DeltaTableConfig("s3a://vkreddy-lakehouse-bronze-aws/delta/customers",     ["_date_part"],    "aws-bronze-customers"),
    DeltaTableConfig("s3a://vkreddy-lakehouse-bronze-aws/delta/transactions",  ["_date_part"],    "aws-bronze-transactions"),
    # Silver
    DeltaTableConfig("s3a://vkreddy-lakehouse-silver/delta/customers",         ["customer_id"],   "silver-customers"),
    DeltaTableConfig("s3a://vkreddy-lakehouse-silver/delta/transactions",      ["customer_id", "txn_date"], "silver-transactions"),
    DeltaTableConfig("s3a://vkreddy-lakehouse-silver/delta/products",          ["category"],      "silver-products"),
    # Gold
    DeltaTableConfig("s3a://vkreddy-lakehouse-gold/delta/daily_revenue",       ["txn_date"],      "gold-daily-revenue"),
    DeltaTableConfig("s3a://vkreddy-lakehouse-gold/delta/customer_rfm",        ["rfm_segment"],   "gold-customer-rfm"),
    DeltaTableConfig("s3a://vkreddy-lakehouse-gold/delta/product_performance", ["category"],      "gold-product-perf"),
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LakehouseMaintenance")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def optimize_table(spark: SparkSession, cfg: DeltaTableConfig) -> None:
    logger.info(f"[{cfg.label}] Starting OPTIMIZE ...")
    dt = DeltaTable.forPath(spark, cfg.path)
    opt = dt.optimize()
    if cfg.zorder_cols:
        opt.executeZOrderBy(*cfg.zorder_cols)
    else:
        opt.executeCompaction()

    logger.info(f"[{cfg.label}] VACUUM (retain {VACUUM_HOURS}h) ...")
    dt.vacuum(VACUUM_HOURS)

    history = dt.history(3).select("version", "timestamp", "operation").collect()
    for row in history:
        logger.info(f"  [{cfg.label}] v{row.version} | {row.operation} | {row.timestamp}")


def main():
    spark = build_spark()
    errors = []
    for cfg in TABLES:
        try:
            optimize_table(spark, cfg)
        except Exception as e:
            logger.error(f"Failed [{cfg.label}]: {e}")
            errors.append(cfg.label)
    spark.stop()
    if errors:
        raise RuntimeError(f"Optimization failed for: {errors}")


if __name__ == "__main__":
    main()
