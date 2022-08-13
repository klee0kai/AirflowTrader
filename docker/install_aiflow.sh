#!/bin/bash


# mkdir $AIRFLOW_HOME

export DEBIAN_FRONTEND=noninteractive

# yes | apt-get install systemctl
yes | apt-get update
yes | apt-get install nano
yes | apt-get install python3
yes | apt-get install python3-pip
yes | apt-get install postgresql postgresql-contrib
yes | apt-get install iputils-ping
yes | pip3 install apache-airflow 
yes | pip3 install psycopg2-binary
yes | pip3 install lxml
# cat airflow_constraints-3.6.txt | xargs -n 1 pip3 install  2>/dev/null
cat requirements.txt | xargs -n 1 pip3 install  2>/dev/null

airflow db init

python3 config_airflow.py








