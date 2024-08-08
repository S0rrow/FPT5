from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ssh_command(command):
    ssh_command = f'ssh hadoop@node3 \"{command}\"'
    
    try:
        result = subprocess.run(ssh_command, shell=True, check=True, capture_output=True)
        print("SSH command output:", result.stdout.decode('utf-8'))
        return result.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        print("Error executing SSH command:", e)
        raise

with DAG(
    'ssh_run_app_subprocess',
    default_args=default_args,
    description='Run Python script on remote server via SSH using PythonOperator and subprocess',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    activate_venv_and_run_app = PythonOperator(
        task_id='activate_venv_and_run_app',
        python_callable=run_ssh_command,
        op_args=['source ~/repository/airflow_test/venv/bin/activate && ~/repository/airflow_test/venv/bin/python ~/repository/airflow_test/app.py']
    )

    notify_result = BashOperator(
        task_id='notify_result',
        bash_command='echo "App execution completed"',
        depends_on_past=True,
        trigger_rule='all_success',
    )

    activate_venv_and_run_app >> notify_result
