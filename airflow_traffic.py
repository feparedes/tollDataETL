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
    task_id = 'unzip_data',                                   # task id
    bash_command = 'tar -xf tolldata.tgz -C ./raw-data',      # task command
    dag = dag                                                 # attached dag
)

"""
    Use cut command in order to get
        - RowID (col 1),
        - Timestamp (col 2),
        - Anonymized Vehicle Number (col 3),
        - Vechile type (col 4)
    delimited by a ','
"""
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',                                                  # task id
    bash_command = 'cut -d"," -f1,2,3,4 raw-data/vehicle-data.csv > csv_data.csv',    # task command
    dag = dag                                                                           # attached dag
)

"""
    Use cut command in order to get
        - Number of axles (col 5),
        - Tollplaza id (col 6),
        - Tollplaza code (col 7),
    delimited by a '\t'
"""
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',                                                                # task id
    bash_command = "cut -d$'\t' -f5,6,7 raw-data/tollplaza-data.tsv | tr '\t' ',' > tsv_data.csv",    # task command
    dag = dag                                                                                         # attached dag
)

"""
    Use cut command in order to get
        - Types of payments code (characters 59-61),
        - Vehicle Code (characters 62-69)
    where each field occupies a fixed number os characters
"""
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',                                                  # task id
    bash_command = 'cut -c59-67 raw-data/payment-data.txt | tr " " "," > fixed_width_data.csv'  # task command
    dag = dag                                                                                   # attached dag
)

"""
    Use paste command in order to get unified csv_data.csv, tsv_data.csv and fixed_width_data.csv
    using comma as a delimiter
"""
consolidate_data = BashOperator(
    task_id = 'consolidate_data',                                                                   # task id
    bash_command = 'paste -d"," csv_data.csv tsv_data.cs fixed_width_data.csv>extracted_data.csv'   # task command
    dag = dag                                                                                       # attached dag
)

"""
    Transform data setting to upper case vehicle_type field and save it into a filed names transform_data.csv
"""
transform_data = BashOperator(
    task_id = 'transform_data',                                                         # task id
    bash_command = 'tr [:lower:] [:upper:] < extracted_data.csv > transform_data.csv'   # task command
    dag = dag                                                                           # attached dag
)

