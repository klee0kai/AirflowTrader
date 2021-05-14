import os, sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import configs

from extract.moex_info import extractMoexInfo

with DAG('Trader_Extract_Moex',
         schedule_interval=timedelta(days=2),
         start_date=datetime.utcnow(),
         ) as dag:
    extractMoexInfo = PythonOperator(
        task_id='moex_info',
        python_callable=extractMoexInfo
    )
