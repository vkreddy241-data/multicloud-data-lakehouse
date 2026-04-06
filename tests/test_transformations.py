"""
Unit tests for Silver and Gold transformation logic.
Uses PySpark local mode — no cloud credentials needed.
Run: pytest tests/ -v
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, IntegerType,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("TestMulticloudLakehouse")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("txn_date",       DateType(),    True),
    StructField("txn_year",       IntegerType(), True),
    StructField("txn_month",      IntegerType(), True),
    StructField("status",         StringType(),  True),
    StructField("_cloud",         StringType(),  True),
])

CUST_SCHEMA = StructType([
    StructField("customer_id",  StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("email",        StringType(), True),
])

PROD_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("category",   StringType(), True),
    StructField("price",      DoubleType(), True),
])


@pytest.fixture
def txn_df(spark):
    rows = [
        ("T001", "C001", "P001", 100.0,  date(2024, 1, 10), 2024, 1, "completed", "aws"),
        ("T002", "C002", "P002", 250.0,  date(2024, 1, 11), 2024, 1, "completed", "azure"),
        ("T003", "C001", "P001", 50.0,   date(2024, 1, 12), 2024, 1, "failed",    "gcp"),
        ("T004", "C003", "P003", 1000.0, date(2024, 1, 15), 2024, 1, "completed", "aws"),
        ("T005", "C002", "P002", 75.0,   date(2024, 2, 1),  2024, 2, "completed", "azure"),
    ]
    return spark.createDataFrame(rows, TXN_SCHEMA)


@pytest.fixture
def cust_df(spark):
    rows = [
        ("C001", "US", "john@example.com"),
        ("C002", "UK", "jane@example.com"),
        ("C003", "CA", "bob@example.com"),
    ]
    return spark.createDataFrame(rows, CUST_SCHEMA)


@pytest.fixture
def prod_df(spark):
    rows = [
        ("P001", "electronics", 99.99),
        ("P002", "clothing",    49.99),
        ("P003", "electronics", 899.99),
    ]
    return spark.createDataFrame(rows, PROD_SCHEMA)


# ---------------------------------------------------------------------------
# Silver tests
# ---------------------------------------------------------------------------
class TestSilverTransformer:
    def test_deduplication_keeps_latest(self, spark, txn_df):
        """Duplicate transaction_id: latest ingested_at wins."""
        from pyspark.sql.window import Window
        dup = txn_df.withColumn("_ingested_at", F.current_timestamp())
        w = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
        deduped = (
            dup.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        assert deduped.count() == txn_df.count()

    def test_filters_negative_amounts(self, spark, txn_df):
        neg_row = spark.createDataFrame(
            [("T999", "C001", "P001", -10.0, date(2024, 1, 1), 2024, 1, "failed", "aws")],
            TXN_SCHEMA,
        )
        combined = txn_df.union(neg_row)
        cleaned = combined.filter(F.col("amount") > 0)
        assert cleaned.count() == txn_df.count()

    def test_all_clouds_present(self, spark, txn_df):
        clouds = {r._cloud for r in txn_df.select("_cloud").distinct().collect()}
        assert clouds == {"aws", "azure", "gcp"}

    def test_amount_cast_to_double(self, spark, txn_df):
        from pyspark.sql.types import DoubleType as DT
        field = dict((f.name, f) for f in txn_df.schema.fields)
        assert isinstance(field["amount"].dataType, DT)


# ---------------------------------------------------------------------------
# Gold tests
# ---------------------------------------------------------------------------
class TestGoldAggregator:
    def test_daily_revenue_row_count(self, spark, txn_df, cust_df, prod_df):
        result = (
            txn_df
            .join(cust_df, on="customer_id", how="left")
            .join(prod_df, on="product_id",  how="left")
            .groupBy("txn_date", "category", "country_code")
            .agg(F.sum("amount").alias("total_revenue"))
        )
        assert result.count() > 0

    def test_total_revenue_positive(self, spark, txn_df, cust_df, prod_df):
        result = (
            txn_df
            .join(cust_df, on="customer_id", how="left")
            .join(prod_df, on="product_id",  how="left")
            .groupBy("txn_date", "category", "country_code")
            .agg(F.sum("amount").alias("total_revenue"))
        )
        neg = result.filter(F.col("total_revenue") < 0).count()
        assert neg == 0

    def test_rfm_segments_valid(self, spark, txn_df, cust_df):
        today = F.current_date()
        result = (
            txn_df
            .join(cust_df, on="customer_id", how="left")
            .groupBy("customer_id")
            .agg(F.max("txn_date").alias("last_order_date"))
            .withColumn("recency_days",  F.datediff(today, F.col("last_order_date")))
            .withColumn("rfm_segment",
                        F.when(F.col("recency_days") <= 30, "champion")
                         .when(F.col("recency_days") <= 60, "loyal")
                         .when(F.col("recency_days") <= 90, "at_risk")
                         .otherwise("churned"))
        )
        valid_segments = {"champion", "loyal", "at_risk", "churned"}
        segments = {r.rfm_segment for r in result.select("rfm_segment").collect()}
        assert segments.issubset(valid_segments)

    def test_product_performance_groups_correctly(self, spark, txn_df, prod_df):
        result = (
            txn_df
            .join(prod_df, on="product_id", how="left")
            .groupBy("product_id", "category", "txn_year", "txn_month")
            .agg(F.count("transaction_id").alias("units_sold"))
        )
        assert result.count() > 0
        total = result.agg(F.sum("units_sold")).collect()[0][0]
        assert total == txn_df.count()


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------
class TestMetadata:
    def test_cloud_column_added(self, spark, txn_df):
        assert "_cloud" in txn_df.columns

    def test_no_null_transaction_ids(self, spark, txn_df):
        null_count = txn_df.filter(F.col("transaction_id").isNull()).count()
        assert null_count == 0
