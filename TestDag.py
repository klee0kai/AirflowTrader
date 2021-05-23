import os, sys

from airflow.sensors.time_delta import TimeDeltaSensor

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

args = {
    'owner': 'airflow',
}

now = datetime.utcnow()


def skip_every_2_minute(owner):
    if datetime.utcnow().minute % 2 == 0:
        raise AirflowSkipException()

    print(F"owner = {owner}")


with DAG(
        dag_id='Test_TimeSensorTestDag',
        default_args=args,
        schedule_interval=timedelta(minutes=1),
        start_date=now - timedelta(minutes=1),
        max_active_runs=1
) as dag:
    skipMinute_op = PythonOperator(
        task_id='skip_minute',
        op_kwargs={'owner':'ds'},
        python_callable=skip_every_2_minute
    )

    run_this = BashOperator(
        task_id='run_this',
        bash_command="echo 'minute tick'",
    )

    skipMinute_op >> run_this

    run_this_every_start = BashOperator(
        task_id='run_this_tick',
        bash_command="echo tick",
    )

if __name__ == "__main__":
    dag.cli()
