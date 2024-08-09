from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client.models import V1VolumeMount, V1Volume, V1PersistentVolumeClaim
from datetime import timedelta

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
        cmds=["python", "/mnt/data/airflow/test.py"],
        name='run-script',
        task_id='run_script_from_pvc',
        volume_mounts=[
            V1VolumeMount(
                name='airflow-worker-pvc',
                mount_path='/mnt/data/airflow'
            )
        ],
        volumes=[
            V1Volume(
                name='airflow-worker-pvc',
                persistent_volume_claim=V1PersistentVolumeClaim(
                    claim_name='airflow-worker-pvc'
                )
            )
        ]
    )
