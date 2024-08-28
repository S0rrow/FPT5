from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

volume_mount = k8s.V1VolumeMount(
    name="airflow-worker-pvc",
    mount_path="/mnt/data/airflow",
    sub_path=None,
    read_only=False
)

volume = k8s.V1Volume(
    name="airflow-worker-pvc",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-worker-pvc"),
)

with DAG(
    dag_id='wanted_preprocessing',
    default_args=default_args,
    description="activate dag every 11'o KST to preprocess jobkorea crawl data",
    schedule_interval='0 11 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    first_preprocessing = KubernetesPodOperator(
        task_id='first_preprocessing_wanted',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:latest',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/wanted_preprocessing/runner.sh"],
        name='first_preprocessing_wanted',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag,
    )
    first_preprocessing