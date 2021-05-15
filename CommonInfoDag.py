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

from extract.moex_info import extractMoexInfoAsync

with DAG('Trader_Extract_Moex',
         schedule_interval=timedelta(hours=1),
         start_date=datetime.utcnow(),
         ) as dag:
    weekSensor = TimeDeltaSensor(
        delta=timedelta(days=7)
    )

    extractMoexInfo = PythonOperator(
        task_id='moex_info',
        python_callable=extractMoexInfoAsync
    )

    weekSensor >> extractMoexInfo
