from datetime import timedelta
# Import DAG Directed Acyclic Graph
from airflow import DAG
# Import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# We define below the data path
DATA_PATH = '~/Data/TollData/raw-data/'

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
    bash_command = 'cd '+DATA_PATH+' && tar -xf ../tolldata.tgz -C ./',      # task command
    dag = dag                                                 # attached dag
)

"""
    Use cut command in order to get
        - RowID (col 1),
        - Timestamp (col 2),
        - Anonymized Vehicle Number (col 3),
        - Vehicle type (col 4)
    delimited by a ','
"""
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',                                                  # task id
    bash_command = 'cut -d"," -f1,2,3,4 '+DATA_PATH+'vehicle-data.csv > '+DATA_PATH+'csv_data.csv',    # task command
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
    bash_command = "cut -d$'\t' -f5,6,7 "+DATA_PATH+"tollplaza-data.tsv | tr '\t' ',' > "+DATA_PATH+"tsv_data.csv",    # task command
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
    bash_command = 'cut -c59-67 '+DATA_PATH+'payment-data.txt | tr " " "," > '+DATA_PATH+'fixed_width_data.csv',  # task command
    dag = dag                                                                                   # attached dag
)

"""
    Use paste command in order to get unified csv_data.csv, tsv_data.csv and fixed_width_data.csv
    using comma as a delimiter
"""
consolidate_data = BashOperator(
    task_id = 'consolidate_data',                                                                   # task id
    bash_command = 'paste -d"," '+DATA_PATH+'csv_data.csv '+DATA_PATH+'tsv_data.csv '+DATA_PATH+'fixed_width_data.csv > '+DATA_PATH+'extracted_data.csv',   # task command
    dag = dag                                                                                       # attached dag
)

"""
    Transform data setting to upper case vehicle_type field and save it into a filed names transform_data.csv
"""
transform_data = BashOperator(
    task_id = 'transform_data',                                                         # task id
    bash_command = 'tr [:lower:] [:upper:] < '+DATA_PATH+'extracted_data.csv > '+DATA_PATH+'transform_data.csv',   # task command
    dag = dag                                                                           # attached dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data