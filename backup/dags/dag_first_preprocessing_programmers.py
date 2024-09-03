from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
    dag_id='preprocessing_programmers',
    default_args=default_args,
    description="프로그래머스 크롤링 데이터 전처리를 위한 DAG (매일 오후 11시 실행)",
    schedule_interval='0 23 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_script = KubernetesPodOperator(
        task_id='first_preprocessing_programmers',
        namespace='airflow',
        image='apache/airflow:2.9.3',
        cmds=["/bin/bash", "-c"],
        arguments=["python /mnt/data/airflow/programmers_preprocessing/pro_preprocessing.py"],
        name='first_preprocessing_programmers',
        volume_mounts=[volume_mount],
        volumes=[volume]
    )
