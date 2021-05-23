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
        schedule_interval=timedelta(minutes=3),
        start_date=days_ago(2),
        max_active_runs=1
) as dag:
    with DAG(
            dag_id=dag.dag_id + ".commonDag",
            default_args=args,
            schedule_interval=timedelta(minutes=10),
            start_date=days_ago(2),
            max_active_runs=1
    ) as subdag_common:
        run_this_10 = BashOperator(
            task_id=subdag_common.dag_id + '-run_this',
            bash_command="echo '10 minute tick'",
        )



    run_this = BashOperator(
        task_id='run_this',
        bash_command="echo 'minute tick'",
    )

    subdag_common >> run_this


if __name__ == "__main__":
    dag.cli()
