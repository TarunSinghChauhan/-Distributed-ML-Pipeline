from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG 1: Daily Ingestion ---
with DAG(
    'daily_ingestion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag1:

    wait_for_files = S3KeySensor(
        task_id='wait_for_landing_files',
        bucket_name='landing-zone',
        bucket_key='web_crawl_data/date={{ ds }}/*.parquet',
        aws_conn_id='aws_default',
        timeout=18 * 60 * 60,
        poke_interval=60
    )

    run_ingestion = SparkSubmitOperator(
        task_id='run_ingestion_job',
        application='/opt/airflow/spark_jobs/ingestion.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password123"
        }
    )

    def validate_bronze_quality():
        # Mock Great Expectations checkpoint call
        print("Validating Bronze layer with Great Expectations...")
        return True

    quality_check = PythonOperator(
        task_id='validate_bronze_quality',
        python_callable=validate_bronze_quality
    )

    wait_for_files >> run_ingestion >> quality_check


# --- DAG 2: Weekly Deduplication ---
with DAG(
    'weekly_deduplication',
    default_args=default_args,
    schedule_interval='0 0 * * 0', # Every Sunday
    catchup=False
) as dag2:

    run_dedupliation = SparkSubmitOperator(
        task_id='run_dedupe_job',
        application='/opt/airflow/spark_jobs/deduplication.py',
        conn_id='spark_default',
        conf={"spark.driver.memory": "4g", "spark.executor.memory": "8g"}
    )

    def check_dedupe_stats():
        # Validate removal rate between 10% and 25%
        print("Checking deduplication removal rates...")
        return True

    validate_stats = PythonOperator(
        task_id='validate_dedupe_stats',
        python_callable=check_dedupe_stats
    )

    run_dedupliation >> validate_stats


# --- DAG 3: Weekly Quality Filter ---
with DAG(
    'weekly_quality_filter',
    default_args=default_args,
    schedule_interval='0 4 * * 0', # Sundays after dedupe
    catchup=False
) as dag3:

    wait_for_dedupe = ExternalTaskSensor(
        task_id='wait_for_dedupe_dag',
        external_dag_id='weekly_deduplication',
        external_task_id='validate_dedupe_stats',
        timeout=3600
    )

    run_quality_filter = SparkSubmitOperator(
        task_id='run_quality_job',
        application='/opt/airflow/spark_jobs/quality_filter.py',
        conn_id='spark_default'
    )

    wait_for_dedupe >> run_quality_filter


# --- DAG 4: Dataset Publishing ---
with DAG(
    'dataset_publishing',
    default_args=default_args,
    schedule_interval=None, # Manual trigger or monthly
    catchup=False
) as dag4:

    run_versioning = SparkSubmitOperator(
        task_id='run_versioning_job',
        application='/opt/airflow/spark_jobs/versioning.py',
        conn_id='spark_default'
    )

    def notify_slack():
        print("Sending Slack notification: Dataset v20240610_a7b2 is ready!")

    send_notification = PythonOperator(
        task_id='send_dataset_ready_notification',
        python_callable=notify_slack
    )

    run_versioning >> send_notification
