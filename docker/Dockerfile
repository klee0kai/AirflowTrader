# This is a comment

FROM ubuntu:latest

LABEL maintainer="klee0kai@gmail.com"

ENV AIRFLOW_HOME=./airflow
# ENV AIRFLOW_VERSION=2.0.1
# ENV PYTHON_VERSION=3.8
# For example: 3.6
# ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.6.txt
COPY install_aiflow.sh install_aiflow.sh
COPY start_airflow.sh start_airflow.sh
COPY constraints-3.6.txt airflow_constraints-3.6.txt
COPY requirements.txt requirements.txt
COPY config_airflow.py config_airflow.py


RUN apt-get update
RUN yes | apt-get upgrade

RUN bash install_aiflow.sh


CMD bash start_airflow.sh



# https://habr.com/ru/post/547936/