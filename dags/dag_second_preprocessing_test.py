from datetime import timedelta
import json
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
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

sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
queue_url = Variable.get('sqs_target_id_msg_url')

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
    dag_id='test_dag2',
    default_args=default_args,
    description="test second preprocessing.",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:
    

    # 변경 2: KubernetesPodOperator의 arguments가 올바르게 리스트로 수정됨
    second_preprocessing = KubernetesPodOperator(
        task_id='second_preprocessing',
        namespace='airflow',
        image='ghcr.io/abel3005/second_preprocessing:1.0',
        cmds=["/bin/bash", "-c"],
        # "/bin/bash", "-c", "sh ..." 형식으로 수정됨
        arguments=["sh", "/mnt/data/airflow/second_preprocessing/runner.sh", "{{ task_instance.xcom_pull(task_ids='message_check', key='id_list') }}"],  # 수정됨
        name='second_preprocessing',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag
    )
    

# 종속성 설정 부분은 변경 없음
second_preprocessing