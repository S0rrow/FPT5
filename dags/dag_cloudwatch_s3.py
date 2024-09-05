from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from datetime import timedelta
import json, boto3
import datetime

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
    dag_id='send_logdata_s3',
    default_args=default_args,
    description="log data transfer to s3.",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    send_log = KubernetesPodOperator(
        task_id='send_log',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:1.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/logging/runner.sh"],
        name='send_log',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag,
        trigger_rule='all_success',  # first_preprocessing이 성공했을 때만 실행
    )
    
    send_log 
