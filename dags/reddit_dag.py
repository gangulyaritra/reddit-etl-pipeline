import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    "owner": "Aritra Ganguly",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 7),
    "email": ["aritraganguly.msc@protonmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id="reddit_etl_pipeline",
    description="ETL Pipeline Design from Reddit API to AWS.",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["reddit", "etl", "pipeline"],
)

extract_reddit_data = PythonOperator(
    task_id="extract_reddit_data",
    python_callable=reddit_pipeline,
    op_kwargs={
        "file_name": f"reddit_data_{file_postfix}",
        "subreddit": Variable.get("subreddit"),
        "time_filter": "day",
        "limit": 100,
    },
    dag=dag,
)

upload_data_s3 = PythonOperator(
    task_id="upload_data_s3", python_callable=upload_s3_pipeline, dag=dag
)

extract_reddit_data >> upload_data_s3
