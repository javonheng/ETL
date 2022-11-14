from airflow.models import Variable
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
from airflow import DAG, settings
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import datetime

default_args = {
    'owner': 'javon',
    'start_date': datetime.datetime.today(),
    'email': 'jjavonhengg@gmail.com',
    'emailonfailure': True,
    'emailonretry': True,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    dag_id='ETL_check_directory',
    default_args=default_args,
    description='Check directory and items in directory',
    schedule_interval='*/30 * * * *', # every 30 secs
    catchup=False,
    tags=['Test']
)

with dag:
    # Define operators
    task1_test_directory = BashOperator(
        task_id='test_directory',
        # Note the space at the end of the command!
        bash_command='pwd',
        cwd='/opt/airflow/scripts/'
        # since the env argument is not specified, this instance of the
        # BashOperator has access to the environment variables of the Airflow
        # instance like AIRFLOW_HOME
    )


    task2_check_items = BashOperator(
        task_id='check_items',
        # Note the space at the end of the command!
        bash_command='ls',
        cwd='/opt/airflow/scripts/'
        # since the env argument is not specified, this instance of the
        # BashOperator has access to the environment variables of the Airflow
        # instance like AIRFLOW_HOME
    )

task1_test_directory >> task2_check_items
