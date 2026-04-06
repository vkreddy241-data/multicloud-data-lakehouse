# Multi-Cloud Data Lakehouse

![CI](https://github.com/vkreddy241-data/multicloud-data-lakehouse/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20EMR%20%7C%20Glue-FF9900?logo=amazonaws)
![Azure](https://img.shields.io/badge/Azure-ADLS%20%7C%20ADB%20%7C%20ADF-0078D4?logo=microsoftazure)
![GCP](https://img.shields.io/badge/GCP-GCS%20%7C%20Dataproc%20%7C%20BigQuery-4285F4?logo=googlecloud)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8)
![Terraform](https://img.shields.io/badge/Terraform-AWS%20%7C%20Azure%20%7C%20GCP-7B42BC?logo=terraform)

A production-grade **multi-cloud data lakehouse** spanning AWS, Azure, and GCP — ingesting data from all 3 clouds independently, unifying into a single Silver Delta Lake on S3, and serving Gold aggregations back to all clouds for BI consumption.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│  AWS S3          │  Azure ADLS Gen2    │  GCP GCS           │
│  (s3_ingestor)   │  (adls_ingestor)    │  (gcs_ingestor)    │
│  Delta + Parquet │  Delta + Parquet    │  Delta + Parquet    │
└──────────────────┴─────────────────────┴────────────────────┘
                            │
                            ▼ (Spark cross-cloud union)
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (S3 — primary)             │
│  silver_transformer.py                                      │
│  - Union from all 3 bronze sources                          │
│  - Deduplication (row_number over PK + ingested_at)         │
│  - Business rules, type casting, standardisation            │
│  - Delta Lake MERGE upsert                                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (replicated to all clouds)   │
│  gold_aggregator.py                                         │
│  ├── daily_revenue       (S3 + GCS + ADLS)                  │
│  ├── customer_rfm        (S3 + GCS + ADLS)                  │
│  └── product_performance (S3 + GCS + ADLS)                  │
└─────────────────────────────────────────────────────────────┘
                            │
               ┌────────────┼────────────┐
               ▼            ▼            ▼
          Redshift      BigQuery      Synapse
          + Athena    (ext. tables)  Analytics
               │            │            │
               └────────────┴────────────┘
                            │
                     Power BI / Looker
```

## Key Features

| Feature | Detail |
|---|---|
| **3-cloud ingestion** | AWS S3, Azure ADLS Gen2, GCP GCS — parallel daily runs |
| **Unified Silver layer** | Cross-cloud deduplication with Delta Lake MERGE |
| **Gold to all clouds** | Daily revenue, Customer RFM, Product performance |
| **Delta Lake** | ACID transactions, time-travel, schema evolution |
| **IaC** | Separate Terraform stacks for AWS, Azure, GCP |
| **Orchestration** | Airflow with TaskGroups for parallel cloud ingestion |
| **dbt** | Semantic layer on top of Silver for Redshift consumers |

## Project Structure

```
multicloud-data-lakehouse/
├── ingestion/
│   ├── aws/s3_ingestor.py          # S3 → Bronze Delta (EMR)
│   ├── azure/adls_ingestor.py      # ADLS Gen2 → Bronze Delta (Databricks)
│   └── gcp/gcs_ingestor.py         # GCS → Bronze Delta (Dataproc)
├── transform/
│   ├── spark/
│   │   ├── silver_transformer.py   # Cross-cloud Bronze → Silver unification
│   │   └── gold_aggregator.py      # Silver → Gold (3 models × 3 clouds)
│   └── dbt/
│       ├── models/staging/stg_transactions.sql
│       ├── models/marts/mart_revenue_by_cloud.sql
│       └── tests/schema.yml
├── storage/delta/
│   └── optimize_all.py             # OPTIMIZE + VACUUM + Z-ORDER all layers
├── orchestration/airflow/dags/
│   └── lakehouse_dag.py            # Daily DAG with cross-cloud TaskGroup
├── infra/terraform/
│   ├── aws/    (S3, EMR, Glue Catalog)
│   ├── azure/  (ADLS, Databricks, ADF)
│   └── gcp/    (GCS, Dataproc, BigQuery)
├── tests/
│   └── test_transformations.py     # 12 pytest unit tests (local Spark)
└── .github/workflows/ci.yml
```

## Quick Start

```bash
pip install -r requirements.txt
pytest tests/ -v    # 12 tests, no cloud credentials needed
```

## Deploy

```bash
# AWS
cd infra/terraform/aws && terraform init && terraform apply

# Azure
cd infra/terraform/azure && terraform init && terraform apply

# GCP
cd infra/terraform/gcp && terraform init \
  -var="gcp_project=your-project-id" && terraform apply
```

## Gold Layer Queries

```sql
-- BigQuery: top revenue categories last 30 days
SELECT category, SUM(total_revenue) AS revenue
FROM `vkreddy-data-platform.lakehouse_gold.daily_revenue`
WHERE txn_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY category ORDER BY revenue DESC;

-- Redshift: churned customers by country
SELECT country_code, COUNT(*) AS churned_customers
FROM lakehouse_gold.customer_rfm
WHERE rfm_segment = 'churned'
GROUP BY country_code ORDER BY churned_customers DESC;
```

## Tech Stack

**Clouds:** AWS (S3, EMR, Glue) · Azure (ADLS Gen2, Databricks, ADF) · GCP (GCS, Dataproc, BigQuery)
**Storage:** Delta Lake 3.0 (Bronze/Silver/Gold)
**Processing:** PySpark 3.5, Spark Structured APIs
**Transformation:** dbt-redshift 1.7
**Orchestration:** Apache Airflow 2.8 (TaskGroups, cross-cloud)
**IaC:** Terraform 1.5 (3 separate provider stacks)
**CI/CD:** GitHub Actions

---
Built by [Vikas Reddy Amaravathi](https://linkedin.com/in/vikas-reddy-a-avr03) — Azure Data Engineer @ Cigna
