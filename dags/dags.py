#importing the libraries
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

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
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = days_ago(1)
)
create_table_sql_query = """ 
CREATE TABLE IF NOT EXISTS Trajectory(Id serial primary key, Track_ID TEXT NOT NULL, 
Type varchar(400) NOT null,
Traveled_Dis varchar(400) NOT null, AVg_Speed TEXT DEFAULT NULL ,Longuited TEXT NOT NULL, 
Latitude TEXT NOT NULL, AVG_Speed TEXT NOT NULL,
Lon_Acc TEXT DEFAULT NULL, Lat_Acc float TEXT DEFAULT NULL, Time TEXT DEFAULT NULL);
"""
# Set up Airflow Task using the Postgres Operator
insert_data_sql_query ="""
COPY Trajectory(Track_ID, Type, Traveled_Dis, AVg_Speed, Longuited, Latitude, Speed, Lon_Acc, Lat_Acc, Time)
FROM ../Data/cleanData.csv
DELIMITER ,
CSV HEADER;
"""
# excuting
create_Table = PostgresOperator(
    sql =create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_localhost",
    dag = dag_psql
    )
  # inseritng to cars using postgres operator
insert_Data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "postgres_localhost",
    dag = dag_psql
)
# confeguring dependencies 
create_Table >> insert_Data

# if __name__ == "__main__":
#     dag_psql.cli()