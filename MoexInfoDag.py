import os, sys
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


# from extract.moex_info import extractMoexInfo


def hi():
    print("hi")


with DAG('Trader Extract Moex',
         schedule_interval=timedelta(days=2),
         start_date=datetime.utcnow(),
         ) as dag:
    extractMoexInfo = PythonOperator(
        task_id='moex_info',
        python_callable=hi
    )
