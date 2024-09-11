from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
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

sqs_client = boto3.client('sqs', region_name='ap-northeast-2')

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
def analyze_message(**context):
    messages = context['ti'].xcom_pull(task_ids='wait_for_lambda_message')
    if messages:
        for message in messages:
            message_body = json.loads(message['Body'])
            if message_body.get('site_symbol') == 'PRO' and message_body.get('status') == 'SUCCESS':
                context['ti'].xcom_push(key='receipt_handle', value=message['ReceiptHandle'])
                return True  # 조건을 만족하면 다음 태스크를 실행
    return False  # 조건을 만족하지 않으면 다음 태스크를 실행하지 않음

# 메시지 삭제 함수
def delete_message_from_sqs(**context):
    receipt_handle = context['ti'].xcom_pull(task_ids='analyze_message', key='receipt_handle')
    if receipt_handle:
        sqs_client.delete_message(
            QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/533267279103/first-preprocessing-message-queue',
            ReceiptHandle=receipt_handle
        )
        #print(f"Message with ReceiptHandle {receipt_handle} deleted successfully.")
    # else:
    #     print("No ReceiptHandle found, skipping message deletion.")

with DAG(
    dag_id='programmers_first_preprocessing',
    default_args=default_args,
    description="activate dag when lambda crawler sended result message.",
    start_date=days_ago(1),
    schedule_interval='0 17 * * * *',
    max_active_runs=1,
    catchup=False,
) as dag:
    
    wait_for_message = SqsSensor(
        task_id='wait_for_lambda_message',
        sqs_queue='https://sqs.ap-northeast-2.amazonaws.com/533267279103/first-preprocessing-message-queue',
        max_messages=4,
        wait_time_seconds=20,
        poke_interval=10,
        delete_message_on_reception=False,
        timeout=3600,
        aws_conn_id='sqs_event_handler_conn',
        region_name='ap-northeast-2',
    )
    
    start_analyze_message = PythonOperator(
        task_id='analyze_message',
        python_callable=analyze_message,
        provide_context=True,
    )
    
    first_preprocessing = KubernetesPodOperator(
        task_id='first_preprocessing_programmers',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:2.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/programmers_preprocessing/runner.sh"],
        name='first_preprocessing_programmers',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag,
        do_xcom_push=True,
        trigger_rule='all_success',  # 이전 작업이 성공하면 실행
    )
    
    delete_message = PythonOperator(
        task_id='delete_sqs_message',
        python_callable=delete_message_from_sqs,
        provide_context=True,
        trigger_rule='all_success',  # first_preprocessing이 성공했을 때만 실행
    )
    
    trigger_2nd_preprocessing = TriggerDagRunOperator(
        task_id='trigger_second_preprocessing',
        trigger_dag_id='second_preprocessing',   # 실행할 second_preprocessing DAG의 DAG ID
        #conf={"records": "{{ task_instance.xcom_pull(task_ids='first_preprocessing_programmers') }}"},  # XCom 출력값 전달
        wait_for_completion=False,  # True로 설정하면 second_preprocessing DAG가 완료될 때까지 현재 DAG 대기
        trigger_rule='all_success',
    )
    
    wait_for_message >> start_analyze_message >> first_preprocessing >> delete_message >> trigger_2nd_preprocessing
