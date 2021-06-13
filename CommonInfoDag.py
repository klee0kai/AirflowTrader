import logging
import os, sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.DEBUG)

from moex.extract.moex_info import extractMoexAllCommonInfo
from moex.extract.moex_hist import extractHists
from moex.extract.moex_api import extractMoexApi
from moex.transform.moex_info_transform import transformMoexCommon

now = datetime.utcnow()

DAG_INTERVAL = timedelta(hours=6)

with DAG('Trader_Extract_Moex',
         schedule_interval=DAG_INTERVAL,
         start_date=now - DAG_INTERVAL,
         max_active_runs=1
         ) as dag:
    dag_extractMoexInfo = PythonOperator(
        task_id='moex_info',
        op_kwargs={
            'interval': timedelta(days=14),
            'airflow': True
        },
        python_callable=extractMoexAllCommonInfo
    )

    dag_extractMoexApiInfo = PythonOperator(
        task_id='moex_api',
        op_kwargs={
            'interval': timedelta(days=14),
            'airflow': True
        },
        python_callable=extractMoexApi
    )

    dag_transformMoexInfo = PythonOperator(
        task_id='moex_info_transform',
        python_callable=transformMoexCommon
    )

    dag_extractMoexHists = PythonOperator(
        task_id='moex_hist',
        python_callable=extractHists
    )

    dag_extractMoexInfo >> dag_transformMoexInfo
    dag_transformMoexInfo >> dag_extractMoexHists
