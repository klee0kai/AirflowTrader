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
from moex.transform.moex_hist_transform_1 import transfromHist1
from moex.transform.moex_indicators_transform import loadAllIndicators
from moex.load.daily_strategy import moex_macd_strategy
from moex.post_load.security_daily_predicts import postLoadSecPredicts

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

    dag_transformMoexHist1 = PythonOperator(
        task_id='moex_hist_transform1',
        python_callable=transfromHist1
    )

    dag_transformMoexHistIndicators = PythonOperator(
        task_id='moex_hist_indicators',
        op_kwargs={
            'airflow': True
        },
        python_callable=loadAllIndicators
    )

    dag_dailyMoexMacd = PythonOperator(
        task_id="moex_daily_macd",
        op_kwargs={
            'airflow': True
        },
        python_callable=moex_macd_strategy.loadDailyMacdStrategy
    )

    dag_SecurityDailyReport = PythonOperator(
        task_id="moex_daily_sec_report",
        op_kwargs={
            'airflow': True
        },
        python_callable=postLoadSecPredicts
    )

    dag_extractMoexInfo >> dag_transformMoexInfo >> dag_extractMoexHists
    dag_extractMoexHists >> dag_transformMoexHist1 >> dag_transformMoexHistIndicators

    dag_transformMoexHistIndicators >> dag_dailyMoexMacd >> dag_SecurityDailyReport
