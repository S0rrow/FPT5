from datetime import timedelta
import json, boto3, logging
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

# 변경 1: BranchPythonOperator에서 task_ids='catch_sqs_message'로 수정
def message_check_handler(**context):
    try:
        # SQS 메시지를 XCom에서 가져옴
        response = context['ti'].xcom_pull(task_ids='catch_sqs_message')
        
        # 로그 추가
        logging.info(f"Received SQS Response: {response}")
        
        if response and 'Messages' in response and len(response['Messages']) > 0:
            message = response['Messages'][0]
            message_body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            
            # Receipt handle 저장
            context['ti'].xcom_push(key='receipt_handle', value=receipt_handle)
            
            # 메시지 body에서 'records' 추출
            records = message_body.get('records')
            logging.info(f"Extracted records: {records}")
            
            if records:
                ids = ','.join(map(str, [record['id'] for record in records]))
                context['ti'].xcom_push(key='id_list', value=ids)
                return 'second_preprocessing'
            else:
                logging.info("No records found in message body.")
                return 'skip_second_preprocessing'
        else:
            logging.info("No valid messages found.")
            return 'skip_second_preprocessing'
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return 'skip_second_preprocessing'


# 메시지 삭제 시 catch_sqs_message에서 receipt_handle을 가져오도록 수정됨
def delete_message_from_sqs(**context):
    receipt_handle = context['ti'].xcom_pull(task_ids='catch_sqs_message', key='receipt_handle')  # 수정됨
    if receipt_handle:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

with DAG(
    dag_id='second_preprocessing',
    default_args=default_args,
    description="Activate DAG when 1st pre-processing DAG ends. This DAG executes with LLM API for pre-processing.",
    start_date=days_ago(1),
    schedule_interval=None,
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
        region_name='ap-northeast-2'
    )
    
    message_check = BranchPythonOperator(
        task_id='message_check',
        python_callable=message_check_handler
    )

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
    
    skip_second_preprocessing = DummyOperator(
        task_id='skip_second_preprocessing'
    )

    delete_message = PythonOperator(
        task_id='delete_sqs_message',
        python_callable=delete_message_from_sqs,
        trigger_rule='all_success'
    )

catch_sqs_message >> message_check
message_check >> [second_preprocessing, skip_second_preprocessing]
second_preprocessing >> delete_message
skip_second_preprocessing >> delete_message