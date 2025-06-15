from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bq import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(
    dag_id="clickstream_etl",
    schedule_interval="0 * * * *",   # hourly
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:

    load_raw = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="{{ var.value.raw_bucket }}",
        source_objects=["clickstream/*.json"],
        destination_project_dataset_table="raw.clickstream",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
    )

    transform_sql = BigQueryInsertJobOperator(
        task_id="transform",
        configuration={
            "query": {
                "query": """
                   CREATE OR REPLACE TABLE analytics.sessions AS
                   SELECT user_id,
                          COUNTIF(event='add_to_cart') AS add_to_cart_events,
                          COUNTIF(event='purchase')    AS purchases,
                          APPROX_QUANTILES(latency_ms, 100)[SAFE_OFFSET(50)] AS p50_latency
                   FROM  `raw.clickstream`
                   WHERE _PARTITIONTIME = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
                   GROUP BY user_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    load_raw >> transform_sql
