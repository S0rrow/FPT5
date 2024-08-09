from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='run_script_from_pvc_dag', 
    default_args=default_args, 
    schedule_interval=None,
    catchup=False,
    ) as dag:
    
    run_script = KubernetesPodOperator(
        namespace='airflow',
        image='apache/airflow:2.9.3',
        cmds=["python", "/mnt/data/airflow/testers/test.py"],
        name='run-script',
        task_id='run_script_from_pvc',
        volume_mounts=[
            {
                'name': 'airflow-worker-pvc',
                'mountPath': '/mnt/data/airflow'
            }
        ],
        volumes=[
            {
                'name': 'airflow-worker-pvc',
                'persistentVolumeClaim': {
                    'claimName': 'airflow-worker-pvc'
                }
            }
        ]
    )
