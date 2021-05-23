import os, sys

from airflow.sensors.time_delta import TimeDeltaSensor

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

now = datetime.utcnow()

with DAG(
        dag_id='Test_TimeSensorTestDag',
        default_args=args,
        schedule_interval=timedelta(minutes=1),
        start_date=now - timedelta(minutes=1),
        max_active_runs=1
) as dag:
    minuteSensor = TimeDeltaSensor(
        task_id='minute',
        delta=timedelta(minutes=2)
    )

    run_this = BashOperator(
        task_id='run_this',
        bash_command="echo 'minute tick'",
    )

    minuteSensor >> run_this

    run_this_every_start = BashOperator(
        task_id='run_this_tick',
        bash_command="echo tick",
    )

if __name__ == "__main__":
    dag.cli()
