from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Biến để kiểm soát xem dữ liệu đã được chèn vào cơ sở dữ liệu hay chưa
data_inserted = False

# Định nghĩa hàm để đọc dữ liệu từ CSV và chèn vào SQLite
def insert_sqlite_from_csv():
    global data_inserted
    if not data_inserted:
        engine = create_engine('sqlite:////opt/airflow/dags/db.sqlite3')
        
        df = pd.read_csv("/opt/airflow/dags/results.csv")
        df.rename(columns={'UserID': 'user_id'}, inplace=True)
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Chèn dữ liệu vào SQLite
        df.to_sql('crud_simulation', engine, if_exists='append', index=False)
        
        data_inserted = True

default_args = {
    'owner': 'NhơnVõ',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'max_retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='Customer_Prosensity_DAG',
    default_args=default_args,
    start_date=datetime(2024, 5, 23),
    schedule_interval='@daily',  # Không lên lịch lại tự động
    dagrun_timeout=timedelta(minutes=1)
) as dag:
    starting = BashOperator(
        task_id='starting',
        bash_command='echo "starting data . . ."'
    )
    
    create_account_table = SQLExecuteQueryOperator(
        task_id='create_account_table',
        conn_id='sqlite_default',
        sql="""
            CREATE TABLE IF NOT EXISTS Account (
                username TEXT,
                password TEXT,
                user_ID TEXT PRIMARY KEY,
                gender CHAR(1),
                email TEXT,
                is_admin BOOLEAN,
                is_user BOOLEAN
            );
        """
    )

    create_dbstrore_table = SQLExecuteQueryOperator(
        task_id='create_dbstrore_table',
        conn_id='sqlite_default',
        sql="""
            CREATE TABLE IF NOT EXISTS DBStore (
                user_ID TEXT,
                basket_icon_click INTEGER,
                basket_add_list INTEGER,
                basket_add_detail INTEGER,
                sort_by INTEGER,
                image_picker INTEGER,
                account_page_click INTEGER,
                promo_banner_click INTEGER,
                detail_wishlist_add INTEGER,
                list_size_dropdown INTEGER,
                closed_minibasket_click INTEGER,
                checked_delivery_detail INTEGER,
                checked_returns_detail INTEGER,
                sign_in INTEGER,
                saw_checkout INTEGER,
                saw_sizecharts INTEGER,
                saw_delivery INTEGER,
                saw_account_upgrade INTEGER,
                saw_homepage INTEGER,
                device_computer INTEGER,
                device_tablet INTEGER,
                returning_user INTEGER,
                loc_uk INTEGER,
                propensity INTEGER,
                score REAL,  
                FOREIGN KEY (user_ID) REFERENCES Account(user_ID)
            );
        """
    )


    create_prosensity_table = SQLExecuteQueryOperator(
        task_id='create_prosensity_table',
        conn_id='sqlite_default',
        sql="""
            CREATE TABLE IF NOT EXISTS Prosensity (
                user_ID TEXT,
                VIP_soon BOOLEAN,
                score REAL,
                FOREIGN KEY (user_ID) REFERENCES Account(user_ID)
            );
        """
    )

    # Task để chèn dữ liệu từ CSV vào bảng DBStore
    insert_data = PythonOperator(
        task_id='insert_sqlite_from_csv',
        python_callable=insert_sqlite_from_csv
    )

    success = BashOperator(
        task_id='success',
        bash_command='echo "success data . . ."'
    )

    # Sắp xếp các task theo thứ tự phù hợp
    starting >> create_account_table >> create_dbstrore_table >> create_prosensity_table >> insert_data >> success
