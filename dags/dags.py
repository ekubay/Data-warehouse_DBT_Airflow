#importing the libraries
import airflow
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import pendulum

# configuring default airflow
args={'owner':'ekubay'}

default_args = {
    'owner': 'ekubay',    
    'email': ['axutec14@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# instantiating DAG
dag_psql = DAG(
    dag_id = "postgres_Dwh",
    default_args=args,
    schedule_interval='@daily',	
    #dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date=datetime(2022,9,10,3)
)
# create_table_sql_query = """ 
# CREATE TABLE IF NOT EXISTS Trajectory(Id serial primary key, Track_ID TEXT NOT NULL, 
# Type varchar(400) NOT null,
# Traveled_Dis varchar(400) NOT null, AVg_Speed TEXT DEFAULT NULL ,Longuited TEXT NOT NULL, 
# Latitude TEXT NOT NULL, Speed TEXT NOT NULL,
# Lon_Acc TEXT DEFAULT NULL, Lat_Acc TEXT DEFAULT NULL, Time TEXT DEFAULT NULL);
# """

create_Table = PostgresOperator(
    sql ="sql/create_table.sql",
    task_id = "create_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )

load_data = PostgresOperator(
    dag=dag_psql,
    task_id="populate_data",
    sql="sql/insert.sql",
    postgres_conn_id="postgres_local",
)

create_Table >> load_data
