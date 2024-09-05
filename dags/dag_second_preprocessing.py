from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from datetime import timedelta
import json, logging

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

def output_conf(**kwargs):
    # TriggerDagRunOperator로부터 받은 데이터를 가져옴
    #records = kwargs['dag_run'].conf.get('records', '')
    conf = kwargs['dag_run'].conf
    logging.info(f"DAG Run conf: {conf}")

with DAG(
    dag_id='second_preprocessing',
    default_args=default_args,
    description="activate dag when 1st pre-processing DAG ended. This Dag execute with LLM API for pre-processing.",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    test_conf_log = PythonOperator(
        task_id='log_conf',
        python_callable=output_conf,
        provide_context=True,
    )
    
    test_conf_log