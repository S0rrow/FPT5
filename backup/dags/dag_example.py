from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from airflow.exceptions import AirflowException
import logging

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

def process_api_request(**context):
    try:
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            api_params = dag_run.conf
        else:
            api_params = {}
        
        param1 = api_params.get('param1', 'default_value')
        param2 = api_params.get('param2', 'default_value')
        
        Variable.set("param1", param1)
        Variable.set("param2", param2)
        
        logging.info(f"Received parameters: param1={param1}, param2={param2}")
        return "API request processed successfully"
    except Exception as e:
        logging.error(f"Error processing API request: {str(e)}")
        raise AirflowException("Failed to process API request")

with DAG(
    dag_id='run_script_from_pvc_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
) as dag:
    
    process_api_request = PythonOperator(
        task_id='process_api_request',
        python_callable=process_api_request,
        provide_context=True,
        dag=dag
    )
    
    run_script = KubernetesPodOperator(
        task_id='run_script_from_pvc',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:latest',
        cmds=["/bin/bash", "-c"],
        arguments=["/mnt/data/airflow/venv/bin/python /mnt/data/airflow/testers/test.py"],
        name='run-script',
        volume_mounts=[volume_mount],
        volumes=[volume],
        image_pull_policy='Always',  # 항상 최신 이미지를 가져오도록 설정
        is_delete_operator_pod=True,  # 작업 완료 후 pod 삭제
        get_logs=True,  # 로그 가져오기
        dag=dag
    )
    
    process_api_request >> run_script