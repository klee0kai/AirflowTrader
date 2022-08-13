#!/bin/bash

echo "starting airflow"

airflow db init


yes | airflow users create \
    --username admin \
    --firstname 'kee0kai' \
    --lastname 'lastname' \
    --role Admin \
    --email 'klee0kai@gmail.com' \
    -p pppp

airflow webserver --port 8085 >> /var/log/airflow_webserver.log | airflow scheduler >> /var/log/airflow_webserver.log
