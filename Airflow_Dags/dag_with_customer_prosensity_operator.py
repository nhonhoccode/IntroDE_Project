from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator


def insert_postgres_from_csv():
        engine = create_engine('postgresql://airflow:airflow@host.docker.internal:5432/customer1')

        
        # Reading CSV file
        df = pd.read_csv("/opt/airflow/dags/results.csv")
        
        # Inserting data into PostgreSQL
        df.to_sql('crud_simulation', engine, if_exists='append', index=False)


default_args = {
    'owner': 'NhơnVõ',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'max_retry_delay': timedelta(minutes=2) # Thêm giá trị này để giữ cho retry_delay không thay đổi
}


with DAG(
    dag_id = 'Customer_Prosensity_DAG',
    default_args=default_args,
    start_date = datetime(2024,5,12),
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=1)
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql ="""
            CREATE TABLE IF NOT EXISTS Account (
                username VARCHAR,
                password VARCHAR,
                user_ID VARCHAR PRIMARY KEY,
                gender CHAR(1),
                email VARCHAR,
                is_admin BOOLEAN,
                is_user BOOLEAN
            );

            CREATE TABLE IF NOT EXISTS DBStore (
                user_ID VARCHAR,
                basket_icon_click INT,
                basket_add_list INT,
                basket_add_detail INT,
                sort_by INT,
                image_picker INT,
                account_page_click INT,
                promo_banner_click INT,
                detail_wishlist_add INT,
                list_size_dropdown INT,
                closed_minibasket_click INT,
                checked_delivery_detail INT,
                checked_returns_detail INT,
                sign_in INT,
                saw_checkout INT,
                saw_sizecharts INT,
                saw_delivery INT,
                saw_account_upgrade INT,
                saw_homepage INT,
                device_computer INT,
                device_tablet INT,
                returning_user INT,
                loc_uk INT,
                propensity INT,
                FOREIGN KEY (user_ID) REFERENCES Account(user_ID),
                score DOUBLE PRECISION
            );

            CREATE TABLE IF NOT EXISTS Prosensity (
                user_ID VARCHAR,
                VIP_soon BOOLEAN,
                score FLOAT,
                FOREIGN KEY (user_ID) REFERENCES Account(user_ID)
            );
        """
    )
    
    task2 = PythonOperator(
        task_id='insert_postgres_from_csv',
        python_callable=insert_postgres_from_csv
    )

    task1 >> task2