import logging
import os, sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from configs import TELEGRAM_BOT_TOKEN_RELEASE
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.DEBUG)

from tel_bot.telegram_bot_server import startBotServer

now = datetime.utcnow()

DAG_INTERVAL = timedelta(hours=6)

with DAG('Telegram_bot_trader',
         schedule_interval=DAG_INTERVAL,
         start_date=now - DAG_INTERVAL,
         max_active_runs=1
         ) as dag:
    dag_extractMoexInfo = PythonOperator(
        task_id='telegram_bot',
        op_kwargs={
            'airflow': True,
            'token': TELEGRAM_BOT_TOKEN_RELEASE
        },
        python_callable=startBotServer
    )

if __name__ == "__main__":
    dag.cli()
