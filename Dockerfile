FROM apache/airflow:2.7.1-python3.8

USER root
RUN apt update -y
RUN apt install build-essential libsasl2-dev -y
USER airflow
RUN python3 -m pip install --upgrade pip
RUN pip install --no-cache-dir airflow-exporter[]
RUN pip install --no-cache-dir apache-airflow-providers-common-sql
RUN pip install --no-cache-dir apache-airflow-providers-apache-hive
RUN pip install --no-cache-dir apache-airflow-providers-apache-druid[apache.hive]
RUN pip install --no-cache-dir acryl-datahub-airflow-plugin[plugin-v2]
RUN pip install --no-cache-dir apache-airflow-providers-mongo
RUN pip install --no-cache-dir apache-airflow-providers-postgres
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN chmod -R 777 /opt/airflow/dags/