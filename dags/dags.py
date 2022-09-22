# importing the datetime
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# configuring default airflow
args={'owner': 'ekubay'}

default_args = {
    'owner': 'ekubay',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# instantiating DAG
dag_psql = DAG(
    dag_id = "postgres_demo",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)
# Set up Airflow Task using the Postgres Operator

create_table_sql_query = """ 
CREATE TABLE cars (id int NOT null primary key , track_id int not null, vehicle_type varchar(500) NOT null,
traveled_d varchar(500) NOT null, avg_speed float NOT null, lat float NOT null, lon float NOT null, speed float NOT null,
loan_acc float NOT null, lat_acc float NOT null, record_time float NOT null);
"""
# insert_data_sql_query = """
# insert into employee (id, name, dept) values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),(3, 'binny','projectmanager'),
# (4, 'omair','projectmanager') ;"""
insert_data_sql_query ="""
COPY cars(track_id, vehicle_type, traveled_d, avg_speed, lat, lon, speed, loan_acc, lat_acc, record_time)
FROM ../Data/cleanData.csv
"""
# DELIMITER ','
# CSV HEADER;
  # creating table 
create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )
  # inseritng 
insert_data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql

# confeguring dependencies 
create_table >> insert_data

    if __name__ == "__main__":
        dag_psql.cli()
    
# The DAG object; we'll need this to instantiate a DAG
#from airflow import DAG


# creating the dag object with context manager
# with DAG(
#     'my_dag',
#     start_date = datetime(2022, 1, 1)
#     default_args=default_args,
#     schedule_interval = "@daily",
#     description='DAG code', 
#     catchup = False) as dag:
      

# Operators; we need this to operate!
# from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization