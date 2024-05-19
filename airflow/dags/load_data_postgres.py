from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
import json
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='shippment_data_mongo_to_postgres_dag',
    default_args=default_args,
    description='DAG to transfer data from MongoDB to PostgreSQL',
    start_date=datetime(2024,4,22),
    schedule_interval=timedelta(days=1),
    catchup=False
)

def transfer_mongo_to_postgres():
    # MongoDB setup
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    mongo_db = mongo_client.shipment_db
    mongo_collection = mongo_db.shipments

    # PostgreSQL setup
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cur = conn.cursor()


    # Fetch data from MongoDB
    shipments = mongo_collection.find({})

    # Insert data into PostgreSQL
    for shipment in shipments:
        cur.execute("""
            INSERT INTO shipments (shipment_id, shipment_date, parcels, address)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (shipment_id) DO NOTHING;
        """, (shipment['shipment_id'], shipment['date'], json.dumps(shipment['parcels']), json.dumps(shipment['address'])))
    conn.commit()

    # Close PostgreSQL connection
    cur.close()
    conn.close()

with dag:
    #wait_for_mongo_insert = ExternalTaskSensor(
    #    task_id='wait_for_mongo_insert',
    #    external_dag_id='mongo_insertion_dag',
    #    external_task_id='insert_to_mongo',
    #    timeout=600,
    #    allowed_states=['success'],
    #    failed_states=['failed', 'skipped'],
    #    mode='reschedule',
    #    poke_interval=60,
    #    dag=dag2
    #)

    transfer_data = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_mongo_to_postgres,
        dag=dag
    )

    #wait_for_mongo_insert >> transfer_data