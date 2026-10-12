from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.notifications.sns import SnsNotifier

# ---------- Alerts ----------
SNS_TOPIC_ARN = "arn:aws:sns:ca-central-1:390132056237:orders_pipeline_alert"

failure_alert = SnsNotifier(
    target_arn=SNS_TOPIC_ARN,
    subject="Airflow FAILED: {{ dag.dag_id }}",
    message=(
        "Task {{ ti.task_id }} failed in DAG {{ dag.dag_id }}.\n"
        "Run: {{ run_id }}\n"
        "Logical date: {{ logical_date }}\n"
        "Log: {{ ti.log_url }}"
    ),
    region_name="ca-central-1",
)

# ---------- Settings for every task ----------
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),     # NEW: any single task fails after 20 min
    "on_failure_callback": failure_alert,
}

# ---------- The pipeline ----------
with DAG(
    dag_id="orders_batch_pipeline",
    schedule="0 6 * * *",                           # 06:00 UTC = 2:00 AM Toronto (summer time)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=45),           # NEW: whole run fails after 45 min
    tags=["retail", "glue"],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="orders-batch-etl",
        region_name="ca-central-1",
        wait_for_completion=True,
    )

    run_crawler = GlueCrawlerOperator(
        task_id="run_crawler",
        config={"Name": "orders_crawler"},
        region_name="ca-central-1",
        wait_for_completion=True,
    )

    run_glue_job >> run_crawler