from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def execute_pvc_script():
    exec(open('/mnt/data/airflow/testers/test.py').read())

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='execute_pvc_script_dag', default_args=default_args, schedule_interval=None) as dag:
    
    run_script = PythonOperator(
        task_id='run_pvc_script',
        python_callable=execute_pvc_script,
    )