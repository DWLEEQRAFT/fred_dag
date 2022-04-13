from airflow.operators.bash import BashOperator
from fredapi import Fred ## 어떤 api 를 사용할 것인가
#from fred_api import get_time_series_data
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum
from datetime import timedelta, datetime

__TASK__ = "daily_micro_etl"

KST = pendulum.timezone("Asia/Seoul")
image = "micro_etl"  # qlcnal0211/micro_etl:

default_args = {
    'owner': 'airflow',
    'retries': 3,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": send_message
}



with DAG(
        dag_id=__TASK__,
        default_args=default_args,
        description=__TASK__,
        schedule_interval="@daily",
        start_date=datetime(2022, 1, 27, tzinfo=KST),
        tags=[__TASK__],
) as dag:
    run_batch = DockerOperator(
        task_id='FRED_Batch_Bigquery',
        image='qraftaxe/fred_dag',
        container_name=image,
        api_version='auto',
        auto_remove=True,
        command="python -m fred_batch",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    task_process = []
    ticker_list = []

    #sample Task
    t1 = BashOperator(task_id='print_date',
                      bash_command='date',
                      dag=dag)

    t2 = BashOperator(task_id='sleep',
                      bash_command='sleep 3',
                      dag=dag)

    t3 = BashOperator(task_id='print_whoami',
                      bash_command='whoami',
                      dag=dag)



    t1 >> t2 >> t3









