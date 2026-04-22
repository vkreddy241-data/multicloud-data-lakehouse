# Architecture — Multi-Cloud Data Lakehouse

## Overview

A production-grade unified data lakehouse that ingests from all three major clouds (AWS S3, Azure ADLS Gen2, GCP GCS) into a single Delta Lake Gold layer, enabling cross-cloud analytics from one place.

```
AWS S3                Azure ADLS Gen2          GCP GCS
(s3_ingestor.py)     (adls_ingestor.py)       (gcs_ingestor.py)
       │                     │                      │
       └─────────────────────┼──────────────────────┘
                             ▼
                    Delta Lake — Bronze
                    (raw ingestion, schema-on-read)
                    Partitioned by cloud_source, ingestion_date
                             │
                             ▼
                    PySpark: silver_transformer.py
                    - Standardises schema across all three cloud sources
                    - Deduplicates on transaction_id
                    - Validates amounts, timestamps, currency codes
                    - Writes → Delta Lake Silver
                             │
                             ▼
                    PySpark: gold_aggregator.py
                    - Builds mart_revenue_by_cloud (revenue × cloud × month)
                    - Writes → Delta Lake Gold
                             │
                    dbt models (run on top of Gold)
                    stg_transactions.sql
                    mart_revenue_by_cloud.sql
                    tests/schema.yml
                             │
                             ▼
                    Analytics / BI consumption
                    (reads from Gold Delta tables)

Airflow (lakehouse_dag.py) orchestrates all stages
Terraform provisions cloud storage in all 3 clouds + compute
```

## Key Design Decisions

**Why Delta Lake as the unified format instead of Parquet or Iceberg?**  
Delta provides ACID transactions and schema evolution across all three cloud object stores using only object storage APIs — no external metadata catalog required. This makes it the lowest-common-denominator format that works on S3, ADLS, and GCS identically.

**Why separate ingestors per cloud (s3_ingestor, adls_ingestor, gcs_ingestor) instead of a generic one?**  
Each cloud SDK has different authentication (IAM roles vs. service principals vs. service accounts), different retry semantics, and different optimal read patterns. Separate ingestors keep cloud-specific complexity isolated and make it easy to add features (e.g., S3 event notifications) without touching other cloud paths.

**Why a single Spark job for Silver transformation instead of per-cloud transforms?**  
By the time data reaches Silver, it should be cloud-agnostic. Unifying in one Spark job means the business validation logic (currency codes, amount ranges, deduplication) is defined once and tested once, regardless of source cloud.

**Why dbt on top of Delta Gold instead of doing aggregations in Spark?**  
Gold-layer aggregations are SQL-expressible business logic (GROUP BY, window functions). dbt provides version-controlled, tested, documented SQL models with automatic lineage. Spark is overkill for this layer.

## Data Flow

| Stage | Source → Destination | Trigger |
|---|---|---|
| Ingest (Bronze) | S3 / ADLS / GCS → Delta Bronze | Airflow, daily |
| Transform (Silver) | Delta Bronze → Delta Silver | Airflow, after ingest |
| Aggregate (Gold) | Delta Silver → Delta Gold | Airflow, after Silver |
| dbt models | Delta Gold → Redshift / BI | Airflow, after Gold |

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud storage | AWS S3, Azure ADLS Gen2, GCP GCS |
| Lakehouse format | Delta Lake |
| Processing | PySpark |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Infrastructure | Terraform (AWS + Azure + GCP providers) |
