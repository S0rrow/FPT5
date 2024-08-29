from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.variable import Variable

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
    # API 요청에서 전달된 파라미터 가져오기
    dag_run = context['dag_run']
    api_params = dag_run.conf if dag_run and dag_run.conf else {}
    
    # 파라미터 처리 로직
    param1 = api_params.get('param1', 'default_value')
    param2 = api_params.get('param2', 'default_value')
    
    # 처리된 파라미터를 Variable로 저장
    Variable.set("param1", param1)
    Variable.set("param2", param2)
    
    print(f"Received parameters: param1={param1}, param2={param2}")
    return "API request processed successfully"


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
        op_kwargs={'api_params': {'param1': 'value1', 'param2': 'value2'}},
        dag=dag
    )
    
    run_script = KubernetesPodOperator(
        task_id='run_script_from_pvc',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:latest',
        cmds=["/bin/bash", "-c"],
        args=["python3", "/mnt/data/airflow/testers/test.py"],
        name='run-script',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag
    )
    
    process_api_request >> run_script
