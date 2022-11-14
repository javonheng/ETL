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
    # 'emailonfailure': True,
    # 'emailonretry': True,
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    dag_id='ETL_load_currency_price',
    default_args=default_args,
    description='Track price of currency everyday.',
    schedule_interval='0 0 * * *', # @daily
    catchup=False,
    tags=['Currency', 'Price']
)

with dag:
    # Define operators
    task1_test_airflow = BashOperator(
        task_id='test_airflow',
        # Note the space at the end of the command!
        bash_command='./test_airflow.sh ',
        cwd='/opt/airflow/scripts/'
        # since the env argument is not specified, this instance of the
        # BashOperator has access to the environment variables of the Airflow
        # instance like AIRFLOW_HOME
    )
    task2_load_currency_price = BashOperator(
        task_id='load_currency_price',
        bash_command='python3 ETL_API_to_DWH.py',
        cwd='/opt/airflow/scripts/'
    )


    # task2_load_currency_price = PythonOperator(
    #     task_id='load_currency_price',
    #     python_callable=ETL_API_to_DWH,
    # )

task1_test_airflow >> task2_load_currency_price
