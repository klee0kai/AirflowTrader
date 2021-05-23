import logging
import os, sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
import configs

logging.basicConfig(level=logging.DEBUG)

from extract.moex_info import extractMoexInfoAsync, extractMoexAllCommonInfo

now = datetime.utcnow()

with DAG('Trader_Extract_Moex',
         schedule_interval=timedelta(minutes=30),
         start_date=now - timedelta(minutes=30),
         max_active_runs=1
         ) as dag:
    extractMoexInfo = PythonOperator(
        task_id='moex_info',
        op_kwargs={
            'interwal': timedelta(days=7),
            'airflow': True
        },
        python_callable=extractMoexAllCommonInfo
    )
