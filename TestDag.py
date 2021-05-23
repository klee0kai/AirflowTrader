import os, sys

from airflow.sensors.time_delta import TimeDeltaSensor

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
        dag_id='Test_TimeSensorTestDag',
        default_args=args,
        schedule_interval='0 0 * * *',
        start_date=days_ago(2)
) as dag:
    minuteSensor = TimeDeltaSensor(
        task_id='minute',
        delta=timedelta(minutes=1)
    )

    run_this = BashOperator(
        task_id= 'run_this',
        bash_command="echo 'minute tick'",
    )

    minuteSensor >> run_this

    run_this_every_start = BashOperator(
        task_id='run_this_tick',
        bash_command="echo tick",
    )

if __name__ == "__main__":
    dag.cli()
