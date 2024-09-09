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
        receipt_handle = message['ReceiptHandle']
        context['ti'].xcom_push(key='receipt_handle', value=receipt_handle)  # 메세지 삭제를 위한 메세지 키값 추가
        data = message_body.get('records')
        if data:
            ids = [record['id'] for record in data]
            context['ti'].xcom_push(key='id_list', value=ids)
            return 'second_preprocessing'  # records 안에 id가 있다면 2차 전처리 실행
        else:
            return 'skip_second_preprocessing'  # records 안에 id가 없다면 2차 전처리 스킵
    else:
        return 'skip_second_preprocessing'  # 조건을 만족하지 않으면 2차 전처리 스킵

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
    
    message_check = BranchPythonOperator(
        task_id='message_check',
        python_callable=message_check_handler,
        dag=dag,
        triger_rule='all_success'
    )

    second_preprocessing = KubernetesPodOperator(
        task_id='second_preprocessing',
        namespace='airflow',
        image='ghcr.io/abel3005/first_preprocessing:2.0',
        cmds=["/bin/bash", "-c"],
        arguments=["sh /mnt/data/airflow/second_preprocessing/runner.sh", "{{ task_instance.xcom_pull(task_ids='message_check', key='id_list') }}"],
        name='second_preprocessing',
        volume_mounts=[volume_mount],
        volumes=[volume],
        dag=dag,
    )
    
    # second_preprocessing을 실행하지 않을 때를 위한 DummyOperator
    skip_second_preprocessing = DummyOperator(
        task_id='skip_second_preprocessing',
        dag=dag
    )

    delete_message = PythonOperator(
        task_id='delete_sqs_message',
        python_callable=delete_message_from_sqs,
        trigger_rule='all_success',  # 성공/실패와 상관없이 실행
        dag=dag
    )

catch_sqs_message >> message_check
message_check >> second_preprocessing >> delete_message
message_check >> skip_second_preprocessing >> delete_message