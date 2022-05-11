from datetime import timedelta
# Import DAG Directed Acyclic Graph
from airflow import DAG
# Import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# We define Airflow DAG arguments
default_args = {
    'owner': 'feparedes',
    'start_date': days_ago(0),              # Start today
    'email': ['email@unemailraro.com'],     
    'email_on_failure': True,               # If it fails send an email
    'email_on_retry': True,                 # If it retries to connect send an email
    'retries': 1,                           # Number os retries
    'retry_delay': timedelta(minutes=5),    # Time betwee retries
}

dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description='Using Apache Airflow',
    schedule_interval = timedelta(minutes=5)   # Scheduled interval 
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xf tolldata.tgz -C ./raw-data',
    dag = dag
)

transform_and_load = BashOperator(
    task_id = 'transform',
    bash_command = 'tr ":" "," < /home/feemjo/data/extracted-data.txt > /home/feemjo/data/transformed-data.csv',
    dag = dag
)

extract >> transform_and_load