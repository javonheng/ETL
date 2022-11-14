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
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    dag_id='ETL_archive_and_move_exports',
    default_args=default_args,
    description='Archived past month exported file and move them into the archived folder.',
    schedule_interval='0 3 1 * *', # “At 03:00 on day-of-month 1.”
    catchup=False,
    tags=['archive']
)

with dag:
    # Define operators
    task1_archive_and_move = BashOperator(
        task_id='archive_and_move',
        # Note the space at the end of the command!
        bash_command='./archive_and_move.sh ',
        cwd='/opt/airflow/scripts/export'
        # since the env argument is not specified, this instance of the
        # BashOperator has access to the environment variables of the Airflow
        # instance like AIRFLOW_HOME
    )

    # task2_load_currency_price = PythonOperator(
    #     task_id='load_currency_price',
    #     python_callable=ETL_API_to_DWH,
    # )

task1_archive_and_move
