from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from datetime import timedelta
import json, boto3
import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with open("/mnt/data/airflow/.KEYS/WATCHER_ACCESS_KEY.json", "r") as f:
    key = json.load(f)
with open("/mnt/data/airflow/.KEYS/DATA_SRC_INFO.json", "r") as f:
    bucket_info = json.load(f)
session = boto3.Session(
    aws_access_key_id=key['aws_access_key_id'],
    aws_secret_access_key=key['aws_secret_key'],
    region_name=key['region']
)
currentTime = datetime.datetime.now()
startDate = currentTime - datetime.timedelta(days=1)
endDate = currentTime

fromDate = int(startDate.timestamp() * 1000)
toDate = int(endDate.timestamp() * 1000)

cloud_log = session.client('logs') 

volume_mount = k8s.V1VolumeMount(
    name="airflow-worker-pvc",
    mount_path="/mnt/data/airflow",
    sub_path=None,
    read_only=Fals
)

volume = k8s.V1Volume(
    name="airflow-worker-pvc",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-worker-pvc"),
)
## env
log_bucket_name =bucket_info['log_bucket_name']

def send_log():
    for l in cloud_log.describe_log_groups()['logGroups']:
        response = cloud_log.create_export_task(
            logGroupName= l['logGroupName'],
            fromTime = fromDate,
            to=toDate,
            destination=log_bucket_name,
            destinationPrefix=l['logGroupName']
            )
        taskId = (response['taskId'])
        status = 'RUNNING'
        while status in ['RUNNING','PENDING']:
            response_desc = cloud_log.describe_export_tasks(
                taskId=taskId
            )
            status = response_desc['exportTasks'][0]['status']['code']
with DAG(
    dag_id='send_logdata_s3',
    default_args=default_args,
    description="log data transfer to s3.",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    send_log = PythonOperator(
        task_id='send_log',
        python_callable=send_log,
        provide_context=True,
        trigger_rule='all_success',  # first_preprocessing이 성공했을 때만 실행
        dag=dag
    )
    
    send_log 