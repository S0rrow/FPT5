import farmhash
import pandas as pd
import pytz
import boto3
import json
import os, sys
import datetime
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import re
from decimal import Decimal
import logging

import utils

path = '/mnt/data/airflow/.KEYS/'  # local file path
with open(f'{path}/FIRST_PREPROCESSING_KEY.json', 'r') as f:
    key = json.load(f)
with open(f'{path}/DATA_SRC_INFO.json', 'r') as f:
    storage_info = json.load(f)

# 로깅 설정 복원
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)
logger = logging.getLogger()

# S3 세션 전역 변수로 선언
session = boto3.Session(
    aws_access_key_id=key['aws_access_key_id'],
    aws_secret_access_key=key['aws_secret_key'],
    region_name=key['region']
)
s3 = session.client('s3')

# S3에서 데이터를 가져오는 함수
def import_bucket():
    pull_bucket_name = storage_info['pull_bucket_name']
    data_archive_bucket_name = storage_info['crawl_data_bucket_name']

    try:
        response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix='rocketpunch')
        if 'Contents' not in response:
            logger.warning(f"No objects found in the bucket: {pull_bucket_name}")
            return None
        
        logger.info(f"Object found in the bucket: {pull_bucket_name}")
        all_data = []
        for obj in response['Contents']:
            try:
                s3_response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
                json_context = s3_response['Body'].read().decode('utf-8')
                cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context)
                json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()]
                data = pd.DataFrame(json_list)
                all_data.append(data)

                # 파일 이동 및 삭제
                copy_source = {"Bucket": pull_bucket_name, "Key": obj['Key']}
                logger.info("Start to Copy data from lake to archive")
                s3.copy(copy_source, data_archive_bucket_name, obj['Key'])
                logger.info("Start to Delete data from lake")
                s3.delete_object(Bucket=pull_bucket_name, Key=obj['Key'])
                logger.info(f"Processed and moved file {obj['Key']}")

            except JSONDecodeError as e:
                logger.error(f"JSONDecodeError while processing {obj['Key']}: {e}")
            except ClientError as e:
                logger.error(f"ClientError while accessing S3: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing {obj['Key']}: {e}")

        if all_data:
            logger.info(f"Data successfully imported from bucket {pull_bucket_name}")
            return pd.concat(all_data, ignore_index=True)
        else:
            logger.info(f"No data to import from bucket {pull_bucket_name}")
            return None

    except Exception as e:
        logger.error(f"An error occurred while importing bucket data: {e}")
        raise

# 전처리 함수
def preprocessing(df):
    logger.info("Preprocessing started")
    processed_list = [] 
    for i, data in df.iterrows():
        try:
            processing_dict = {}
            if pd.notnull(data['job_task']):
                processing_dict['job_tasks'] = ' '.join(
                    [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', re.sub(r'\\/', '/', data['job_task'])).split() if item not in ['-', '+']]
                )
            processing_dict['stacks'] = re.sub(r'\\/', '/', data['job_specialties'])
            processing_dict['job_requirements'] = ' '.join(
                [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', re.sub(r'\\/', '/', data['job_detail'])).split() if item not in ['-', '+']]
            )
            processing_dict['indurstry_type'] = re.sub(r'\\/', '/', data['job_industry'])

            date_start = datetime.datetime.strptime(data['date_start'], "%Y.%m.%d")
            processing_dict['start_date'] = int(date_start.timestamp())

            if 'Null' not in data['date_end']:
                date_end = datetime.datetime.strptime(data['date_end'], "%Y.%m.%d")
                processing_dict['end_date'] = int(date_end.timestamp())
            else:
                processing_dict['end_date'] = 'null'

            processing_dict['required_career'] = "신입" in data['job_career']
            processing_dict['site_symbol'] = "RP"
            processing_dict['crawl_url'] = data['job_url']
            processing_dict['crawl_domain'] = data['crawl_domain']

            id = farmhash.Fingerprint32("RP" + data['company_name'] + str(data['job_id']))
            processing_dict['id'] = id

            dt = datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d_%H:%M:%S")
            processing_dict['get_date'] = int(dt.timestamp())

            processed_list.append(processing_dict) 
            logger.info(f"Successfully processed data row {i}")

        except Exception as e:
            logger.error(f"An error occurred during preprocessing row {i}: {e}")
    
    return pd.DataFrame(processed_list)

# 데이터 DynamoDB에 업로드 함수
def upload_data(records):
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table(storage_info['restore_table_name'])
    with table.batch_writer() as batch:
        for item in records:
            batch.put_item(Item=item)

def main():
    try:
        df = import_bucket()

        if df is not None and not df.empty:
            preprocessed_df = preprocessing(df)
            unique_df = preprocessed_df.drop_duplicates(subset='id', keep='first')
            upload_data(unique_df.to_dict(orient='records'))
        else:
            logger.info('No task for preprocessing.')
        sys.exit(0)
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

