#importing the libraries
import airflow
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import pendulum
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd

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
create_table_sql_query = """ 
CREATE TABLE IF NOT EXISTS Trajectory(Id serial primary key, Track_ID TEXT NOT NULL, 
Type varchar(400) NOT null,
Traveled_Dis varchar(400) NOT null, AVg_Speed TEXT DEFAULT NULL ,Longuited TEXT NOT NULL, 
Latitude TEXT NOT NULL, Speed TEXT NOT NULL,
Lon_Acc TEXT DEFAULT NULL, Lat_Acc TEXT DEFAULT NULL, Time TEXT DEFAULT NULL);
"""
# Set up Airflow Task using the Postgres Operator
# insert_data_sql_query ="""
# 'COPY Trajectory(Track_ID, Type, Traveled_Dis, AVg_Speed, Longuited, Latitude, Speed, Lon_Acc, Lat_Acc, Time)'
# "FROM '../Data/cleanData.csv'"
# "DELIMITER ','"
# "CSV HEADER"
# """
# excuting
# default
#engine = create_engine("postgresql://scott:tiger@localhost/mydatabase")

# psycopg2
#engine = create_engine("postgresql+psycopg2://scott:tiger@localhost/mydatabase")

# pg8000
#engine = create_engine("postgresql+pg8000://scott:tiger@localhost/mydatabase")
def migrate_data(path,db_table):
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/trajectory_db')
    # engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow",
    #          echo=True, future=True)
    df = pd.read_csv(path)
    print("start migrating data")
    df.to_sql(db_table, con=engine, if_exists='replace',index_label='id')
    print("migrating completed")


migrate = PythonOperator(
    task_id='migrate',
    dag=dag_psql,
    python_callable=migrate_data,
    op_kwargs={
        "path": "../data/cleanData.csv",
            "db_table":"Trajectory"
        }
    )


read_data = PostgresOperator(
    dag=dag_psql,
    task_id="read_data",
    sql = "sql/read.sql",
    postgres_conn_id="postgres_local"
    )
create_Table = PostgresOperator(
    sql =create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )
  # inseritng to cars using postgres operator

# insert_Data = PostgresOperator(
#     task_id = "insert_task",
#     postgres_conn_id = "postgres_local",
#     sql = insert_data_sql_query,     
#     dag = dag_psql
#     # bash_command=(
#     #     'psql -d trajectory_db -U postgres -c'
#     #     # 'COPY Trajectory(Track_ID, Type, Traveled_Dis, AVg_Speed, Longuited, Latitude, Speed, Lon_Acc, Lat_Acc, Time)'
#     #     # "FROM '../Data-warehouse_DBT_Airflow/Data/cleanData.csv'"
#     #     # "DELIMITER ','"
#     #     # 'CSV HEADER"'
#     #     )  
#     )
#     sql = insert_data_sql_query,
#     task_id = "insert_data_task",
#     postgres_conn_id = "postgres_local",
#     dag = dag_psql
# )
# confeguring dependencies 
# connection


# populate_data = PostgresOperator(
#     dag=dag_psql,
#     #bash_command='psql -d trajectory_db -U postgres -c',
#     sql = "sql/insert.sql",
#     task_id="populate_data",
#     postgres_conn_id="postgres_local",
# )

create_Table >> migrate >> read_data

# if __name__ == "__main__":
#     dag_psql.cli()