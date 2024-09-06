import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# CloudWatch Logs 클라이언트 생성
# client = boto3.client('logs', region_name='ap-northeast-2')  # 서울 리전
import json, boto3


with open("../logging_utils/.KEYS/WATCHER_ACCESS_KEY.json", "r") as f:
    key = json.load(f)
with open("../logging_utils/.KEYS/DATA_SRC_INFO.json", "r") as f:
    bucket_info = json.load(f)

session = boto3.Session(
    aws_access_key_id=key['aws_access_key_id'],
    aws_secret_access_key=key['aws_secret_key'],
    region_name=key['region']
)

client = session.client('logs')



class cloudwatch_handler(logging.Handler):
    log_group_name = None
    log_stream_name = None

    def set_init(self,group_name,stream_name):
        self.log_group_name = group_name
        self.log_stream_name = stream_name
        try:
            client.create_log_group(logGroupName=group_name)
        except client.exceptions.ResourceAlreadyExistsException:
            pass
        try:
            client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
        except client.exceptions.ResourceAlreadyExistsException:
            pass
    
    def emit(self, record):
        log_entry = self.format(record)
        try:
            response = client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[
                    {
                        'timestamp': int(record.created * 1000),
                        'message': log_entry
                    },
                ],
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Credentials error: {e}")
        except Exception as e:
            print(f"Error sending log to CloudWatch: {e}")
