from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from kubernetes.client import models as k8s
from datetime import timedelta
import json, boto3

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

# 메시지 분석 함수
def hello_word(**context):
    print("hello world!")


with DAG(
    dag_id='dag_trigger_test',
    default_args=default_args,
    description="for trigger test",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    start_hello_world = PythonOperator(
        task_id='start_hello_world',
        python_callable=hello_word,
        dag=dag
    )
     
    trigger_sample = TriggerDagRunOperator(
        task_id='trigger_sample',
        trigger_dag_id='test_dag2',   # trigger할 DAG id
        conf={"records": "123,456,789"},  # 만약 airflow 단에서 다른 dag로 보낼 때 사용 보낸 데이터는 트리거 된 dag에서 옵션을 지정해야 쓸 수 있음
        wait_for_completion=False,  # True로 설정하면 second_preprocessing DAG가 완료될 때까지 현재 DAG 대기
        trigger_rule='all_success', # 이전 작업이 성공해야 해당 task 실행
        dag=dag
    )
    
    dummy_test = DummyOperator(
        task_id='dummy_test'
    )
    
    dummy_test >> start_hello_world >> trigger_sample