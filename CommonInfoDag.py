import logging
import os, sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.DEBUG)

from extract.moex.moex_info import extractMoexAllCommonInfo

now = datetime.utcnow()

DAG_INTERVAL = timedelta(days=1)

with DAG('Trader_Extract_Moex',
         schedule_interval=DAG_INTERVAL,
         start_date=now - DAG_INTERVAL,
         max_active_runs=1
         ) as dag:
    extractMoexInfo = PythonOperator(
        task_id='moex_info',
        op_kwargs={
            'interval': timedelta(days=14),
            'airflow': True
        },
        python_callable=extractMoexAllCommonInfo
    )
