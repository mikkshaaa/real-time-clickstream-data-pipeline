from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {'owner': 'you', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG("clickstream_etl", start_date=datetime(2024,1,1), schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    batch_transform = BashOperator(
        task_id="batch_transform",
        bash_command="python /opt/airflow/dags/../../src/batch_transform.py"
    )
    batch_transform
