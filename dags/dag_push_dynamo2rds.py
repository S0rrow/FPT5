from datetime import timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

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
    dag_id='push_dynamo2rds',
    default_args=default_args,
    description="Activate DAG when 1st pre-processing DAG ends. This DAG executes with LLM API for pre-processing.",
    start_date=days_ago(1),
    schedule_interval='0 20 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    dynamodb_to_rds = KubernetesPodOperator(
        task_id='dynamodb_to_rds',
        namespace='airflow',
        image='ghcr.io/abel3005/third_preprocessing:1.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/third_preprocessing/runner.sh"],
        name='dynamodb_to_rds',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag
    )

    dynamodb_to_rds

