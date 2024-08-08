# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     # 'start_date': datetime(2023, 8, 7),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def run_ssh_command(command):
#     ssh_command = f'ssh hadoop@node3 \"{command}\"'
    
#     try:
#         result = subprocess.run(ssh_command, shell=True, check=True, capture_output=True)
#         print("SSH command output:", result.stdout.decode('utf-8'))
#         return result.stdout.decode('utf-8')
#     except subprocess.CalledProcessError as e:
#         print("Error executing SSH command:", e)
#         raise

# with DAG(
#     'programmers_crawler',
#     default_args=default_args,
#     description='A crawling DAG that runs every day at 11 PM',
#     schedule_interval='0 23 * * *',  # Cron expression for 11 PM daily
# ) as dag:

#     start = DummyOperator(
#         task_id='execute_python_file',
#         python_callable=run_ssh_command,
#         op_args=['source ~/.bashrc && ~/home/team3/miniconda3/bin/python ~/repository/airflow_test/app.py']

#     )


# start




from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_from_psw',
    default_args=default_args,
    description='Run main.py every day at 11 PM',
    schedule_interval='*/5 * * * *',
)

run_script = BashOperator(
    task_id='testing',
    bash_command='cd /home/team3/repository/workspaces/psw/ && date >> time.txt',
    dag=dag,
)

run_script
