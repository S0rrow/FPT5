
from datetime import timedelta
import json, boto3
import datetime

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
log_bucket_name =bucket_info['log_bucket_name']

def main():
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
            
if __name__ == "__main__":
    main()