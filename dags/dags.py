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
    #'start_date': airflow.ucleartils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    'email': ['axutec14@gmail.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# instantiating DAG
dag_psql = DAG(
    dag_id = "postgres_demo",
    default_args=args,
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = days_ago(1)
)

# Set up Airflow Task using the Postgres Operator
create_table_sql_query = """ 
CREATE TABLE cars (id serial NOT null primary key , track_id int not null, vehicle_type varchar(400) NOT null,
traveled_d varchar(400) NOT null, avg_speed float NOT null, lat float NOT null, lon float NOT null, speed float NOT null,
loan_acc float NOT null, lat_acc float NOT null, record_time float NOT null);
"""
# insert_data_sql_query = """
# insert into employee (id, name, dept) values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),(3, 'binny','projectmanager'),
# (4, 'omair','projectmanager') ;"""
insert_data_sql_query ="""
COPY cars(track_id, vehicle_type, traveled_d, avg_speed, lat, lon, speed, loan_acc, lat_acc, record_time)
FROM ../Data/cleanData.csv
DELIMITER ,
CSV HEADER;
"""
# creating table  car using the postgres operator
create_Table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_dag",
    dag = dag_psql
    )
  # inseritng to cars using postgres operator
insert_Data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "postgres_dag",
    dag = dag_psql
)
# confeguring dependencies 
create_Table >> insert_Data

# if __name__ == "__main__":
#     dag_psql.cli()