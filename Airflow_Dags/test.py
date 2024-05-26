import datetime as dt
import pandas as pd
import random
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def insertMongoDB():
    client = pymongo.MongoClient("mongodb://admin:admin@mongodb:27017")
    db = client["collection"]
    collection = db['collection_employees']

    df = pd.read_csv("/opt/airflow/dags/employees.csv")
    dfObjects = df.to_dict('records')

    try_count = 0
    success = False
    while try_count < 3 and not success:
        data = random.choice(dfObjects)
        print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "Inserting data…")

        if collection.find_one({"id": data["id"]}):
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "This ID is duplicated")
            try_count += 1
            time.sleep(180)
        else:
            collection.insert_one(data)
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "Insert successfully")
            success = True
            time.sleep(300)

    client.close()

default_args = {
    'owner': 'Nhơn Võ',
    # 'start_date': dt.datetime.now() - dt.timedelta(minutes=2),
    'retries': 5,
    'retry_delay': dt.timedelta(minutes=2)
}

with DAG(
    dag_id = 'Customer_Prosensity_DAG',
    default_args=default_args,
    start_date = datetime(2021,7,29,2),
    scheduler_interval = '@daily'
) as dag:
    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "Inserting data . . ."'
    )

    insertData = PythonOperator(
        task_id='insert_data',
        python_callable=insertMongoDB
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Completed"'
    )

print_starting >> insertData >> end