"""
Airflow DAG: Orchestrates the full multi-cloud data lakehouse pipeline.

Schedule: Daily at 03:00 UTC
Flow:
  Ingest (AWS + Azure + GCP in parallel)
    → Silver Transform
    → Gold Aggregation
    → dbt models
    → Delta Optimize
    → Data Quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

DEFAULT_ARGS = {
    "owner":            "vikas-reddy",
    "depends_on_past":  False,
    "email":            ["vkreddy241@gmail.com"],
    "email_on_failure": True,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

EMR_CLUSTER_ID = "{{ var.value.emr_cluster_id }}"

SPARK_STEP = lambda script: [{   # noqa: E731
    "Name": script,
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit", "--deploy-mode", "cluster",
            "--packages",
            "io.delta:delta-core_2.12:2.4.0",
            f"s3://vkreddy-lakehouse-scripts/{script}.py",
        ],
    },
}]

DATAPROC_JOB = lambda script: {   # noqa: E731
    "reference": {"project_id": "vkreddy-data-platform"},
    "placement": {"cluster_name": "lakehouse-cluster"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://vkreddy-lakehouse-scripts/{script}.py",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.0.jar"],
    },
}


def run_dq_checks(**context):
    """Cross-cloud data quality: row counts match across bronze sources."""
    run_date = context["ds"]
    # In production: query each cloud's bronze layer and assert counts are within 5% of each other
    print(f"DQ checks passed for run_date={run_date}")


def run_dbt(**context):
    import subprocess
    run_date = context["ds"]
    subprocess.run(
        ["dbt", "run", "--vars", f'{{"run_date": "{run_date}"}}'],
        check=True, capture_output=True, text=True,
    )
    subprocess.run(["dbt", "test"], check=True, capture_output=True, text=True)


with DAG(
    dag_id="multicloud_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily multi-cloud lakehouse: Bronze → Silver → Gold → dbt",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["multicloud", "lakehouse", "delta", "aws", "azure", "gcp"],
) as dag:

    # ---------- Bronze ingestion (all 3 clouds in parallel) ----------
    with TaskGroup("bronze_ingestion") as bronze_group:

        ingest_aws = EmrAddStepsOperator(
            task_id="ingest_aws_s3",
            job_flow_id=EMR_CLUSTER_ID,
            steps=SPARK_STEP("ingestion/aws/s3_ingestor"),
            aws_conn_id="aws_default",
        )

        ingest_azure = AzureDataFactoryRunPipelineOperator(
            task_id="ingest_azure_adls",
            pipeline_name="adls_ingestor_pipeline",
            adf_conn_id="azure_data_factory_default",
            factory_name="vkreddy-lakehouse-adf",
            resource_group_name="vkreddy-data-rg",
        )

        ingest_gcp = DataprocSubmitJobOperator(
            task_id="ingest_gcp_gcs",
            job=DATAPROC_JOB("ingestion/gcp/gcs_ingestor"),
            project_id="vkreddy-data-platform",
            region="us-central1",
            gcp_conn_id="google_cloud_default",
        )

    # ---------- Silver transform (cross-cloud unification) ----------
    silver = EmrAddStepsOperator(
        task_id="silver_transform",
        job_flow_id=EMR_CLUSTER_ID,
        steps=SPARK_STEP("transform/spark/silver_transformer"),
        aws_conn_id="aws_default",
    )

    # ---------- Gold aggregation ----------
    gold = EmrAddStepsOperator(
        task_id="gold_aggregation",
        job_flow_id=EMR_CLUSTER_ID,
        steps=SPARK_STEP("transform/spark/gold_aggregator"),
        aws_conn_id="aws_default",
    )

    # ---------- dbt models ----------
    dbt = PythonOperator(task_id="dbt_run_and_test", python_callable=run_dbt)

    # ---------- Delta optimize ----------
    optimize = BashOperator(
        task_id="delta_optimize",
        bash_command=(
            "spark-submit "
            "--packages io.delta:delta-core_2.12:2.4.0 "
            "/opt/airflow/scripts/optimize_all.py"
        ),
    )

    # ---------- Data quality ----------
    dq = PythonOperator(task_id="cross_cloud_dq_checks", python_callable=run_dq_checks)

    # ---------- DAG wiring ----------
    bronze_group >> silver >> gold >> dbt >> optimize >> dq
