from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import timedelta
import json, logging, boto3

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

# message_check_handler에 receipt_handle push 추가
def message_check_handler(**context):
    response = context['ti'].xcom_pull(task_ids='catch_sqs_message')
    if response:
        message = response['Messages'][0]
        message_body = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']  # 추가
        context['ti'].xcom_push(key='receipt_handle', value=receipt_handle)  # 추가
        data = message_body.get('records')
        if data:
            context['ti'].xcom_push(key='data_key', value=data)
            return True  # records 안에 id가 있다면 True
        else:
            return False  # records 안에 id가 없다면 False
    else:
        return False  # 조건을 만족하지 않으면 다음 태스크를 실행하지 않음

def check_massage_check_status(**context):
    # XCom에서 start_message_handling의 결과를 가져옴
    result = context['ti'].xcom_pull(task_ids='message_check')
    if result:
        return 'second_preprocessing'  # True일 경우 second_preprocessing 실행
    else:
        return 'skip_second_preprocessing'  # False일 경우 스킵

# 메시지 삭제 함수
def delete_message_from_sqs(**context):
    receipt_handle = context['ti'].xcom_pull(task_ids='message_check', key='receipt_handle')
    if receipt_handle:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )


with DAG(
    dag_id='second_preprocessing',
    default_args=default_args,
    description="activate dag when 1st pre-processing DAG ended. This Dag execute with LLM API for pre-processing.",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    
    catch_sqs_message = SqsSensor(
        task_id='catch_sqs_message',
        sqs_queue=queue_url,
        max_messages=1,
        wait_time_seconds=20,
        poke_interval=10,
        aws_conn_id='sqs_event_handler_conn',
        region_name='ap-northeast-2',
        dag=dag
    )
    
    message_check = PythonOperator(
        task_id='message_check',
        python_callable=message_check_handler,
        dag=dag
    )
    
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_massage_check_status,
        dag=dag
    )

    second_preprocessing = KubernetesPodOperator(
        task_id='second_preprocessing',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:2.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/second_preprocessing/runner.sh", "{{ task_instance.xcom_pull(task_ids='message_check', key='data_key') }}"],
        name='second_preprocessing',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag,
        trigger_rule='all_success',
    )
    
    # second_preprocessing을 실행하지 않을 때를 위한 DummyOperator
    skip_second_preprocessing = DummyOperator(
        task_id='skip_second_preprocessing',
        dag=dag
    )

    delete_message = PythonOperator(
        task_id='delete_sqs_message',
        python_callable=delete_message_from_sqs,
        trigger_rule='all_done',  # 성공/실패와 상관없이 실행
        dag=dag
    )

catch_sqs_message >> message_check >> branch_task
branch_task >> second_preprocessing >> delete_message
branch_task >> skip_second_preprocessing >> delete_message