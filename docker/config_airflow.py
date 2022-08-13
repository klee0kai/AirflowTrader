#!/bin/python3
import configparser
import shutil

if __name__=="__main__":
    config = configparser.ConfigParser()
    shutil.copyfile('/airflow/airflow.cfg','/airflow/airflow_origin.cfg')
    config.read('/airflow/airflow.cfg')
    config['core']['executor']='LocalExecutor'
    config['core']['sql_alchemy_conn']='postgresql+psycopg2://postgres:pppp@postgres'
    with open('/airflow/airflow.cfg','w') as f:
        config.write(f)


