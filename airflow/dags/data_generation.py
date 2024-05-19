from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python_operator import PythonOperator
import random
import string

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Generate random shipment ID
def generate_shipment_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

# Generate random barcode
def generate_barcode():
    return ''.join(random.choices(string.digits, k=10))

# Generate random address
def generate_address():
    return {
        'street': f"{random.choice(['Main St.', 'Elm St.', 'Oak St.'])} {random.randint(1, 100)}",
        'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
        'zipcode': ''.join(random.choices(string.digits, k=5))
    }

# Generate shipment data
def generate_shipment_data():
    try:
        # Ensure 'mongo_default' matches the Connection ID in Airflow
        mongo_hook = MongoHook(conn_id='mongo_default')
        client = mongo_hook.get_conn()
        db = client.shipment_db
        collection = db.shipments
        print(f"Connected to MongoDB - {client.server_info()}")

        shipments = []
        for _ in range(1000):
            shipment_id = generate_shipment_id()
            date = datetime.now() - timedelta(days=random.randint(1, 30))
            parcels = [{'barcode': generate_barcode()} for _ in range(random.randint(1, 5))]
            address = generate_address()

            shipment = {
                'shipment_id': shipment_id,
                'date': date,
                'parcels': parcels,
                'address': address
            }
            shipments.append(shipment)
        
        collection.insert_many(shipments)
        
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
        raise

with DAG('generate_shipment_data',
         default_args=default_args,
         description='Generate shipment data for MongoDB',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2024, 4, 15),
         catchup=False) as dag:

    generate_data_task = PythonOperator(
        task_id='generate_data_task',
        python_callable=generate_shipment_data,
        dag=dag,
    )

    generate_data_task