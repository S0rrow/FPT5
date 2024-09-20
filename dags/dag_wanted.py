from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

queue_url = Variable.get('sqs_queue_url')
aws_region = Variable.get('aws_region')

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
    messages = context['ti'].xcom_pull(task_ids='wait_for_lambda_message', key='messages')
    if messages:
        for message in messages:
            message_body = json.loads(message['Body'])
            if message_body.get('site_symbol') == 'WAN' and message_body.get('status') == 'SUCCESS':
                context['ti'].xcom_push(key='receipt_handle', value=message['ReceiptHandle'])
                return 'first_preprocessing_wanted'  # 조건을 만족하면 다음 태스크를 실행
    return 'skip_first_preprocessing'  # 조건을 만족하지 않으면 다음 태스크를 실행하지 않음

# 메시지 삭제 함수
def delete_message_from_sqs(**context):
    aws_conn_id = 'sqs_event_handler_conn'
    sqs_hook = SqsHook(aws_conn_id=aws_conn_id, region_name=aws_region)
    sqs_client = sqs_hook.get_conn()
    receipt_handle = context['ti'].xcom_pull(task_ids='analyze_message', key='receipt_handle')
    if receipt_handle:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

with DAG(
    dag_id='wanted_first_preprocessing',
    default_args=default_args,
    description="activate dag when lambda crawler sended result message.",
    start_date=days_ago(1),
    schedule_interval='10 16 * * * *',
    max_active_runs=1,
    catchup=False
) as dag:

    wait_for_message = SqsSensor(
        task_id='wait_for_lambda_message',
        sqs_queue=queue_url,
        max_messages=10,
        num_batches=4,
        wait_time_seconds=20,
        poke_interval=10,
        timeout=600,
        aws_conn_id='sqs_event_handler_conn',
        region_name=aws_region,
        delete_message_on_reception=False,
        dag=dag
    )
    
    start_analyze_message = BranchPythonOperator(
        task_id='analyze_message',
        python_callable=analyze_message,
        dag=dag,
    )

    first_preprocessing = KubernetesPodOperator(
        task_id='first_preprocessing_wanted',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:2.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/wanted_preprocessing/runner.sh"],
        name='first_preprocessing_wanted',
        volume_mounts=[volume_mount],
        volumes=[volume],
        trigger_rule='all_success',  # 이전 작업이 성공하면 실행
        dag=dag,
    )
    
    skip_first_preprocessing = DummyOperator(
        task_id = 'skip_first_preprocessing',
    )

    delete_message = PythonOperator(
        task_id='delete_sqs_message',
        python_callable=delete_message_from_sqs,
        provide_context=True,
        trigger_rule='all_success',  # first_preprocessing이 성공했을 때만 실행
        dag=dag,
    )
     
    trigger_2nd_preprocessing = TriggerDagRunOperator(
        task_id='trigger_second_preprocessing',
        trigger_dag_id='second_preprocessing',   # 실행할 second_preprocessing DAG의 DAG ID
        #conf={"records": "{{ task_instance.xcom_pull(task_ids='first_preprocessing_wanted') }}"},  # XCom 출력값 전달
        wait_for_completion=False,  # True로 설정하면 second_preprocessing DAG가 완료될 때까지 현재 DAG 대기
        trigger_rule='all_success',
        dag=dag,
    )
    
    wait_for_message >> start_analyze_message 
    start_analyze_message >> [first_preprocessing, skip_first_preprocessing]
    first_preprocessing >> delete_message >> trigger_2nd_preprocessing
    skip_first_preprocessing